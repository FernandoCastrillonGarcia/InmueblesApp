from zenml import step
from database import MongoSingleton
from typing import Any, Dict
from tqdm import tqdm

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
            {"SOURCE": source, "WEB_PROPERTY_CODE": code}
            #{"WEB_PROPERTY_CODE": code}
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

