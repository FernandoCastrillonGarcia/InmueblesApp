from zenml import step
from database import MongoSingleton, QdrantSingleton
from typing import Any, Dict
from tqdm import tqdm
import numpy as np
from qdrant_client.models import PointStruct

@step
def remove_duplicates_by_source() -> Dict[str, int]:
    """
    Remove duplicate properties from MongoDB based on 'source' and 'web_property_id' (or 'WEB_PROPERTY_CODE').
    
    The logic is:
    1. Group by 'SOURCE' and 'WEB_PROPERTY_CODE' (or equivalent unique ID).
    2. Keep the most recent record (based on 'scraped_at' or 'batch_id').
    3. Remove others.
    
    Returns:
        Dict with stats of deleted items.
    """
    client = MongoSingleton.get_client()
    db = client["inmuebles_db"]
    collection = db["properties"]
    
    # We will use an aggregation pipeline to find duplicates
    # We group by SOURCE and WEB_PROPERTY_CODE
    pipeline = [
        {
            "$group": {
                "_id": {
                    "source": "$SOURCE",
                    "code": "$WEB_PROPERTY_CODE"
                },
                "ids": {"$push": "$_id"},
                "count": {"$sum": 1},
                "latest_scrape": {"$max": "$scraped_at"}
            }
        },
        {
            "$match": {
                "count": {"$gt": 1}
            }
        }
    ]
    
    duplicates = collection.aggregate(pipeline)
    deleted_count = 0
    groups_processed = 0
    
    for doc in duplicates:
        # doc['ids'] contains all _ids for this duplicate group
        # We need to keep the one that matches the latest_scrape (or just the last one inserted)
        # Since we want to keep the LATEST, we should sort or find the one with max date.
        
        # Let's fetch the docs to be sure which one to keep, or assume ids are somewhat ordered (unsafe).
        # Better approach: Find the doc with the latest_scrape and keep it.
        
        # NOTE: Aggregation output 'ids' is just a list of ObjectIds.
        # We need to query them to compare dates or use a more complex aggregation.
        # However, for simplicity and safety, let's just query the specific group again to sort.
        
        source = doc["_id"]["source"]
        code = doc["_id"]["code"]
        
        # Find all docs for this group, sorted by scraped_at descending (newest first)
        cursor = collection.find(
            {"SOURCE": source, "WEB_PROPERTY_CODE": code},
            {"WEB_PROPERTY_CODE": code}
        ).sort("scraped_at", -1)
        
        docs = list(cursor)
        
        if len(docs) > 1:
            # Keep the first one (newest), delete the rest
            ids_to_delete = [d["_id"] for d in docs[1:]]
            
            if ids_to_delete:
                result = collection.delete_many({"_id": {"$in": ids_to_delete}})
                deleted_count += result.deleted_count
                groups_processed += 1
                
    print(f"✅ Deduplication complete. Removed {deleted_count} duplicate properties across {groups_processed} groups.")
    
    return {"deleted_count": deleted_count, "groups_processed": groups_processed}


@step
def cap_prices_by_property_type(
    collections: list[str],
    local: bool = True
) -> Dict[str, Any]:
    """Cap prices per PROPERTY_TYPE: negatives → 0, values above p99 → p99.

    Applies to both MongoDB and Qdrant (same collection names).

    Args:
        collections: Collection names to process (e.g. ["Arriendo", "Venta"]).
        local: Whether to use local DB instances.

    Returns:
        Dict with per-collection, per-type stats of updates made.
    """
    mongo_client = MongoSingleton.get_client(local=local)
    db = mongo_client["inmuebles_db"]
    qdrant = QdrantSingleton.get_client(local=local)

    stats: Dict[str, Any] = {}

    for col_name in collections:
        collection = db[col_name]
        property_types = collection.distinct("PROPERTY_TYPE")
        col_stats: Dict[str, Dict[str, int]] = {}

        for ptype in property_types:
            # --- Compute p99 for this PROPERTY_TYPE ---
            prices = [
                doc["PRICE"]
                for doc in collection.find(
                    {"PROPERTY_TYPE": ptype, "PRICE": {"$exists": True}},
                    {"PRICE": 1}
                )
                if isinstance(doc.get("PRICE"), (int, float))
            ]

            if not prices:
                continue

            p99 = float(np.percentile(prices, 99))
            negatives_fixed = 0
            capped = 0

            # --- Fix negatives in Mongo ---
            neg_result = collection.update_many(
                {"PROPERTY_TYPE": ptype, "PRICE": {"$lt": 0}},
                {"$set": {"PRICE": 0}}
            )
            negatives_fixed += neg_result.modified_count

            # --- Cap above p99 in Mongo ---
            cap_result = collection.update_many(
                {"PROPERTY_TYPE": ptype, "PRICE": {"$gt": p99}},
                {"$set": {"PRICE": p99}}
            )
            capped += cap_result.modified_count

            # --- Fix in Qdrant (scroll + set_payload) ---
            offset = None
            while True:
                points, offset = qdrant.scroll(
                    collection_name=col_name,
                    scroll_filter={
                        "must": [
                            {"key": "PROPERTY_TYPE", "match": {"value": ptype}}
                        ]
                    },
                    with_payload=["PRICE", "PROPERTY_TYPE"],
                    limit=100,
                    offset=offset,
                )

                for point in points:
                    price = point.payload.get("PRICE")
                    if not isinstance(price, (int, float)):
                        continue

                    new_price = None
                    if price < 0:
                        new_price = 0
                        negatives_fixed += 1
                    elif price > p99:
                        new_price = p99
                        capped += 1

                    if new_price is not None:
                        qdrant.set_payload(
                            collection_name=col_name,
                            payload={"PRICE": new_price},
                            points=[point.id],
                        )

                if offset is None:
                    break

            col_stats[ptype] = {
                "p99": round(p99),
                "negatives_fixed": negatives_fixed,
                "capped_above_p99": capped,
            }
            print(f"  {col_name}/{ptype}: p99={p99:,.0f} | neg→0: {negatives_fixed} | capped: {capped}")

        stats[col_name] = col_stats

    print("✅ Price capping complete.")
    return stats

