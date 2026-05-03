import os
import json
from kfp import dsl

# Configuration for Vertex AI
PROJECT_ID = os.getenv("PROJECT_ID", "inmuebles-app-437-v2")
LOCATION = os.getenv("LOCATION", "us-central1")
PIPELINE_ROOT = f"gs://{PROJECT_ID}-pipeline-roots/inmueblesapp"

# We use our custom pipeline image which contains all dependencies and the utils folder
BASE_IMAGE = f"us-east1-docker.pkg.dev/{PROJECT_ID}/inmuebles-app/pipeline-runner:latest"

@dsl.component(base_image=BASE_IMAGE)
def remove_duplicates_by_source_op(local: bool) -> str:
    from backend.database import MongoSingleton
    import json
    
    client = MongoSingleton(local=local).client
    db = client["inmuebles_db"]
    collection = db["properties"]
    
    pipeline = [
        {"$group": {"_id": {"source": "$SOURCE", "code": "$WEB_PROPERTY_CODE"}, "ids": {"$push": "$_id"}, "count": {"$sum": 1}, "latest_scrape": {"$max": "$scraped_at"}}},
        {"$match": {"count": {"$gt": 1}}}
    ]
    
    duplicates = collection.aggregate(pipeline)
    deleted_count = 0
    groups_processed = 0
    
    for doc in duplicates:
        source = doc["_id"]["source"]
        code = doc["_id"]["code"]
        
        cursor = collection.find({"SOURCE": source, "WEB_PROPERTY_CODE": code}, {"WEB_PROPERTY_CODE": code}).sort("scraped_at", -1)
        docs = list(cursor)
        
        if len(docs) > 1:
            ids_to_delete = [d["_id"] for d in docs[1:]]
            if ids_to_delete:
                result = collection.delete_many({"_id": {"$in": ids_to_delete}})
                deleted_count += result.deleted_count
                groups_processed += 1
                
    print(f"✅ Deduplication complete. Removed {deleted_count} duplicate properties across {groups_processed} groups.")
    return json.dumps({"deleted_count": deleted_count, "groups_processed": groups_processed})


@dsl.component(base_image=BASE_IMAGE)
def remove_erroneous_values_op(collections: list, local: bool) -> str:
    from backend.database import MongoSingleton, QdrantSingleton
    from backend.utils import create_uuid_from_string
    import json
    
    mongo_client = MongoSingleton(local=local).client
    db = mongo_client["inmuebles_db"]
    qdrant = QdrantSingleton(local=local).client

    stats = {}

    for col_name in collections:
        collection = db[col_name]
        deleted_count = 0

        bad_filters = [
            {"FLOOR": 202},
            {"STRATUM": 101},
            {"PROPERTY_TYPE": {"$in": ["Apartamento", "Apartaestudio"]}, "AREA": {"$lte": 0}},
            {"PROPERTY_TYPE": {"$in": ["Apartamento", "Apartaestudio"]}, "AREA": {"$gt": 400}},
            {"PROPERTY_TYPE": {"$in": ["Apartamento", "Apartaestudio"]}, "PRICE": {"$lt": 100_000}},
            {"PROPERTY_TYPE": {"$in": ["Apartamento", "Apartaestudio"]}, "PRICE": {"$gt": 20_000_000}},
        ]

        for bad_filter in bad_filters:
            bad_docs = list(collection.find(bad_filter, {"WEB_PROPERTY_CODE": 1}))
            if not bad_docs:
                continue

            codes = [doc["WEB_PROPERTY_CODE"] for doc in bad_docs if "WEB_PROPERTY_CODE" in doc]
            result = collection.delete_many(bad_filter)
            deleted_count += result.deleted_count

            for code in codes:
                try:
                    point_id = create_uuid_from_string(code)
                    qdrant.delete(collection_name=col_name, points_selector=[point_id])
                except Exception:
                    pass

        stats[col_name] = {"deleted": deleted_count}
        print(f"  {col_name}: removed {deleted_count} erroneous documents")

    print("✅ Erroneous value removal complete.")
    return json.dumps(stats)


@dsl.component(base_image=BASE_IMAGE)
def cap_prices_by_property_type_op(collections: list, local: bool) -> str:
    from backend.database import MongoSingleton, QdrantSingleton
    import numpy as np
    import json
    
    mongo_client = MongoSingleton(local=local).client
    db = mongo_client["inmuebles_db"]
    qdrant = QdrantSingleton(local=local).client

    stats = {}

    for col_name in collections:
        collection = db[col_name]
        property_types = collection.distinct("PROPERTY_TYPE")
        col_stats = {}

        for ptype in property_types:
            prices = [
                doc["PRICE"]
                for doc in collection.find({"PROPERTY_TYPE": ptype, "PRICE": {"$exists": True}}, {"PRICE": 1})
                if isinstance(doc.get("PRICE"), (int, float))
            ]

            if not prices:
                continue

            p99 = float(np.percentile(prices, 99))
            negatives_fixed = 0
            capped = 0

            neg_result = collection.update_many({"PROPERTY_TYPE": ptype, "PRICE": {"$lt": 0}}, {"$set": {"PRICE": 0}})
            negatives_fixed += neg_result.modified_count

            cap_result = collection.update_many({"PROPERTY_TYPE": ptype, "PRICE": {"$gt": p99}}, {"$set": {"PRICE": p99}})
            capped += cap_result.modified_count

            offset = None
            while True:
                points, offset = qdrant.scroll(
                    collection_name=col_name,
                    scroll_filter={"must": [{"key": "PROPERTY_TYPE", "match": {"value": ptype}}]},
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
                        qdrant.set_payload(collection_name=col_name, payload={"PRICE": new_price}, points=[point.id])

                if offset is None:
                    break

            col_stats[ptype] = {"p99": round(p99), "negatives_fixed": negatives_fixed, "capped_above_p99": capped}
            print(f"  {col_name}/{ptype}: p99={p99:,.0f} | neg→0: {negatives_fixed} | capped: {capped}")

        stats[col_name] = col_stats

    print("✅ Price capping complete.")
    return json.dumps(stats)


@dsl.component(base_image=BASE_IMAGE)
def cap_numeric_fields_op(collections: list, local: bool) -> str:
    from backend.database import MongoSingleton, QdrantSingleton
    import numpy as np
    import json
    
    FIELDS_TO_CAP = ["BUILT_AREA", "AREA", "GARAGE", "BATHROOMS", "ROOMS"]
    mongo_client = MongoSingleton(local=local).client
    db = mongo_client["inmuebles_db"]
    qdrant = QdrantSingleton(local=local).client

    stats = {}

    for col_name in collections:
        collection = db[col_name]
        property_types = collection.distinct("PROPERTY_TYPE")
        col_stats = {}

        for ptype in property_types:
            ptype_stats = {}

            for field in FIELDS_TO_CAP:
                values = [
                    doc[field]
                    for doc in collection.find({"PROPERTY_TYPE": ptype, field: {"$exists": True, "$type": "number"}}, {field: 1})
                    if isinstance(doc.get(field), (int, float))
                ]

                if not values:
                    continue

                p99 = float(np.percentile(values, 99))
                negatives_fixed = 0
                capped = 0

                neg_result = collection.update_many({"PROPERTY_TYPE": ptype, field: {"$lt": 0}}, {"$set": {field: 0}})
                negatives_fixed += neg_result.modified_count

                cap_result = collection.update_many({"PROPERTY_TYPE": ptype, field: {"$gt": p99}}, {"$set": {field: p99}})
                capped += cap_result.modified_count

                offset = None
                while True:
                    points, offset = qdrant.scroll(
                        collection_name=col_name,
                        scroll_filter={"must": [{"key": "PROPERTY_TYPE", "match": {"value": ptype}}]},
                        with_payload=[field, "PROPERTY_TYPE"],
                        limit=100,
                        offset=offset,
                    )

                    for point in points:
                        val = point.payload.get(field)
                        if not isinstance(val, (int, float)):
                            continue

                        new_val = None
                        if val < 0:
                            new_val = 0
                        elif val > p99:
                            new_val = p99

                        if new_val is not None:
                            qdrant.set_payload(collection_name=col_name, payload={field: new_val}, points=[point.id])

                    if offset is None:
                        break

                ptype_stats[field] = {"p99": round(p99, 2), "neg_fixed": negatives_fixed, "capped": capped}

            if ptype_stats:
                col_stats[ptype] = ptype_stats

        stats[col_name] = col_stats

    print("✅ Numeric field capping complete.")
    return json.dumps(stats)


@dsl.pipeline(
    name="inmueblesapp-cleaning-pipeline",
    description="Pipeline to clean the MongoDB and Qdrant databases.",
    pipeline_root=PIPELINE_ROOT,
)
def cleaning_pipeline():
    collections = ["Arriendo", "Venta"]
    local = False

    erroneous_task = remove_erroneous_values_op(collections=collections, local=local)
    
    price_task = cap_prices_by_property_type_op(collections=collections, local=local)
    price_task.after(erroneous_task)
    
    numeric_task = cap_numeric_fields_op(collections=collections, local=local)
    numeric_task.after(price_task)
