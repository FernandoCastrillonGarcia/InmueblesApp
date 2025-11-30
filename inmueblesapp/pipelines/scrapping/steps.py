
from concurrent.futures import ProcessPoolExecutor, as_completed
# from qdrant_client.models import VectorParams, Distance
from typing import Tuple, Annotated
from datetime import datetime
from zenml import step
from tqdm import tqdm
from rich import print
import pandas as pd

from pipelines.scrapping.src.finca_raiz import OPERATION_INDEX, PROPERTY_INDEX, LOCAL
import pipelines.scrapping.src.finca_raiz as fr
# from database import QdrantSingleton
# Log to MLflow
import mlflow

from zenml import step
from zenml.client import Client
import pandas as pd
from typing import Annotated, Tuple

@step(experiment_tracker="mlflow_tracker")
def scrape_properties(
    operations:list[str],
    properties:list[str]
)-> Tuple[Annotated[pd.DataFrame, 'points'],Annotated[pd.DataFrame, 'stats']]:

    # qdrant = QdrantSingleton(local = LOCAL).get_client()
    all_items = []
    all_stats = []
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    for operation in operations:

        # create collection if it doesn't exist
        # try:
        #     qdrant.create_collection(operation,
        #         vectors_config=VectorParams(
        #             size=768,
        #             distance=Distance.COSINE
        #         ))
        # except Exception as e:
        #     pass
        
        for property in properties:

            # REquest parameters
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

            # Requests en Multiprocessing
            with ProcessPoolExecutor(max_workers=10) as executor:
                futures = [
                    executor.submit(fr.get_hits, rows, page, property_index, operation_index, None, location) 
                    for page in range(1, pages + 1)
                ]                
                for future in tqdm(as_completed(futures), total=pages, desc="Fetching data"):
                    try:
                        items = future.result()

                        # points = hits_to_points(items)                    
                        # qdrant.upsert(
                        #     collection_name=operation,
                        #     points=points
                        # )

                        total_points += len(items)
                        success_count += 1

                        all_items += items
                    except Exception as e:
                        failure_count += 1
                        print(f"Error fetching page data: {e}")
            print()

            # Return statistics
            signature = {
                # Index of the signature
                "timestamp": timestamp,
                "property": property,
                "operation": operation,

                # scrapping process
                "pages_success": success_count,
                "pages_failed": failure_count,
                "success_rate": success_count / (success_count + failure_count) * 100,
            }

            all_stats.append(signature)

    # Logging stats
    stats = pd.DataFrame(all_stats)

    # Storing Data
    df = pd.DataFrame(all_items)

    df['LATITUDE'] = pd.to_numeric(df['LATITUDE'], errors='coerce')
    df['LONGITUDE'] = pd.to_numeric(df['LONGITUDE'], errors='coerce')
    df['PRICE'] = pd.to_numeric(df['PRICE'], errors='coerce')
    df['AREA'] = pd.to_numeric(df['AREA'], errors='coerce')
    
    # Save with timestamp
    df.to_csv(f"data/raw/properties_{timestamp}.csv", index=False)

    return df, stats

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
    previous_run = runs[1]  # Second latest
    previous_step = previous_run.steps["scrape_properties"]
    
    previous_stats = previous_step.outputs["stats"][0].load()

    comparison = current_stats.merge(
        previous_stats,
        on=['operation', 'property'],
        suffixes=('_current', '_previous'),
        how='outer'  # Include new/removed distributions
    )

    comparison["delta_success"] = comparison['pages_success_current'] - comparison['pages_success_previous']
    comparison["delta_failed"] = comparison['pages_failed_current'] - comparison['pages_failed_previous']

    return comparison[['delta_success', 'delta_failed']]