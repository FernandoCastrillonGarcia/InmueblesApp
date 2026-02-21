
from concurrent.futures import ProcessPoolExecutor, as_completed
from qdrant_client.models import VectorParams, Distance, PointStruct

from typing import Annotated, Any
from datetime import datetime
from zenml import step
from zenml.client import Client
from tqdm import tqdm
from rich import print
import pandas as pd
import mlflow

from pipelines.scrapping.src.finca_raiz import OPERATION_INDEX, PROPERTY_INDEX, LOCAL
import pipelines.scrapping.src.finca_raiz as fr
from database import MongoSingleton, QdrantSingleton
from utils import embed, preprocess_text, create_uuid_from_string
# Source - https://stackoverflow.com/a/62703850
# Posted by Alec Segal, modified by community. See post 'Timeline' for change history
# Retrieved 2026-02-08, License - CC BY-SA 4.0

import os
os.environ["TOKENIZERS_PARALLELISM"] = "false"

@step(experiment_tracker="mlflow_tracker")
def scrape_properties(
    operations:list[str],
    properties:list[str]
)-> Annotated[pd.DataFrame, 'stats']:

    mongo_client = MongoSingleton.get_client(local = LOCAL)
    mongodb = mongo_client["inmuebles_db"]
    qdrant = QdrantSingleton.get_client(local = LOCAL)

    all_stats = []
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    scrape_time = datetime.now()

    for operation in operations:

        all_items = []

        # create collection if it doesn't exist
        try:
            qdrant.create_collection(operation,
                vectors_config=VectorParams(
                    size=768,
                    distance=Distance.COSINE
                ))
        except Exception as e:
            pass
        
        for property in properties:

            # Request parameters
            operation_index = OPERATION_INDEX[operation]
            property_index = PROPERTY_INDEX[property]
            location = fr.get_location('bogota')
            rows = 32

            # get the total pages that are going to be scrapped
            total_hits = fr.get_total_hits(property_index, operation_index, location=location)
            pages = fr.get_total_pages(total_hits, rows)    
            print(f"Operation: {operation}, Property: {property}")
            print(f"Total properties: {total_hits}, Total pages: {pages}")

            # Stats in the scrapping process
            total_points = 0
            success_count = 0
            failure_count = 0

            # ====== Phase 1: Scrape all pages in parallel ======
            scraped_items: list[dict] = []
            with ProcessPoolExecutor(max_workers=8) as executor:
                futures = [
                    executor.submit(fr.get_hits, rows, page, property_index, operation_index, None, location) 
                    for page in range(1, pages + 1)
                ]                
                for future in tqdm(as_completed(futures), total=pages, desc="Fetching data"):
                    try:
                        items = future.result()
                        if items:
                            for item in items:
                                item['scraped_at'] = scrape_time
                                item['batch_id'] = timestamp
                            scraped_items.extend(items)
                        success_count += 1
                    except Exception as e:
                        failure_count += 1
                        print(f"Error fetching page data: {e}")

            # ====== Phase 2: Embed all descriptions in one GPU batch ======
            if scraped_items:
                descriptions = [preprocess_text(item.pop('DESCRIPTION')) for item in scraped_items]
                ids = [create_uuid_from_string(item['WEB_PROPERTY_CODE']) for item in scraped_items]
                vectors = embed(descriptions)

                # ====== Phase 3: Build points and upsert in batches of 200 ======
                UPSERT_BATCH = 200
                points = [
                    PointStruct(id=uid, vector=vec, payload=item)
                    for uid, vec, item in zip(ids, vectors, scraped_items)
                ]
                for i in range(0, len(points), UPSERT_BATCH):
                    qdrant.upsert(collection_name=operation, points=points[i:i + UPSERT_BATCH])
                # mongodb[operation].insert_many(scraped_items)

                total_points = len(scraped_items)
                all_items.extend(scraped_items)

            print()

            if len(all_items) > 0:
                df = pd.DataFrame(all_items)

                df['LATITUDE'] = pd.to_numeric(df['LATITUDE'], errors='coerce')
                df['LONGITUDE'] = pd.to_numeric(df['LONGITUDE'], errors='coerce')
                df['PRICE'] = pd.to_numeric(df['PRICE'], errors='coerce')
                df['AREA'] = pd.to_numeric(df['AREA'], errors='coerce')

                # Return statistics
                if success_count + failure_count == 0:
                    success_rate = None
                else:
                    success_rate = success_count / (success_count + failure_count) * 100
            
                signature = {
                    # Index of the signature
                    "timestamp": timestamp,
                    "property": property,
                    "operation": operation,

                    # scrapping process
                    "pages_success": success_count,
                    "pages_failed": failure_count,
                    "success_rate": success_rate,

                    # numeric statistics
                    'total_properties':len(df.loc[df['PROPERTY_TYPE'] == property]),
                    'mean_price': df.loc[df['PROPERTY_TYPE'] == property, 'PRICE'].mean(),
                }

                all_stats.append(signature)

    # Logging stats
    stats = pd.DataFrame(all_stats)


    print(f"✅ Stored {len(all_items)} properties")

    return stats

@step(experiment_tracker = "mlflow_tracker")
def validate_scrapping_signature(
    current_stats:pd.DataFrame
    )->Annotated[pd.DataFrame, 'comparison']:
    
    client = Client()
    
    # Get previous run (skip current, get second latest)
    runs = client.list_pipeline_runs(
        pipeline_name="scraping_pipeline",
        size=2
    )
    
    if len(runs) < 2:
        print("⚠️ No previous run to compare with")
        mlflow.set_tag("comparison_status", "no_previous_run")
        return current_stats  # Return as-is
    
    # Get previous stats

    tries = 1

    while tries < 5:
        try:
            previous_run = runs[tries]  # TODO: Manejar el caso donde no hay corrida previa
            previous_step = previous_run.steps["scrape_properties"]
            
            previous_stats = previous_step.outputs["stats"][0].load()
            break
        except:
            tries += 1
    
    if tries == 5:
        raise Exception('Despues de 5 intentos hubo un fallo. Hay que revisar')


    comparison = current_stats.merge(
        previous_stats,
        on=['operation', 'property'],
        suffixes=('_current', '_previous'),
        how='outer'  # Include new/removed distributions
    )

    # Scrapping process comparions
    comparison["pages_success_delta"] = comparison['pages_success_current'] - comparison['pages_success_previous']
    comparison["pages_failed_delta"] = comparison['pages_failed_current'] - comparison['pages_failed_previous']

    comparison["mean_price_delta"] = comparison['mean_price_current'] - comparison['mean_price_previous']
    comparison["total_properties_delta"] = comparison['total_properties_current'] - comparison['total_properties_previous']

    return comparison