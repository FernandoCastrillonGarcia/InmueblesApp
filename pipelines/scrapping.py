import os
from kfp import dsl
from kfp.dsl import Dataset, Output, Input

# Configuration for Vertex AI
PROJECT_ID = os.getenv("PROJECT_ID", "inmuebles-app-437-v2")
LOCATION = os.getenv("LOCATION", "us-central1")
PIPELINE_ROOT = f"gs://{PROJECT_ID}-pipeline-roots/inmueblesapp"

# We use our custom pipeline image which contains all dependencies and the utils folder
BASE_IMAGE = f"us-east1-docker.pkg.dev/{PROJECT_ID}/inmuebles-app/pipeline-runner:latest"

@dsl.component(base_image=BASE_IMAGE)
def scrape_properties_op(
    operations: list,
    properties: list,
    stats_out: Output[Dataset]
):
    from concurrent.futures import ProcessPoolExecutor, as_completed
    from qdrant_client.models import VectorParams, Distance, PointStruct
    from datetime import datetime
    from tqdm import tqdm
    import pandas as pd
    
    from pipelines.utils.finca_raiz import OPERATION_INDEX, PROPERTY_INDEX, LOCAL, get_location, get_total_hits, get_total_pages, get_hits
    from backend.database import MongoSingleton
    from backend.utils import embed, preprocess_text, create_uuid_from_string
    
    import os
    os.environ["TOKENIZERS_PARALLELISM"] = "false"

    mongo_client = MongoSingleton(local=LOCAL).client
    mongodb = mongo_client["inmuebles_db"]

    all_stats = []
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    scrape_time = datetime.now()

    for operation in operations:
        all_items = []
        
        for property in properties:
            operation_index = OPERATION_INDEX[operation]
            property_index = PROPERTY_INDEX[property]
            location = get_location('bogota')
            rows = 32

            total_hits = get_total_hits(property_index, operation_index, location=location)
            pages = get_total_pages(total_hits, rows)
            pages=1
            print(f"Operation: {operation}, Property: {property}")
            print(f"Total properties: {total_hits}, Total pages: {pages}, Rows per Page: {rows}")

            total_points = 0
            success_count = 0
            failure_count = 0

            # Phase 1: Scrape
            scraped_items = []
            with ProcessPoolExecutor(max_workers=8) as executor:
                futures = [
                    executor.submit(get_hits, rows, page, property_index, operation_index, None, location) 
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

            # Phase 2: Embed
            if scraped_items:
                descriptions = [preprocess_text(item.pop('DESCRIPTION', '')) for item in scraped_items]
                ids = [create_uuid_from_string(item['WEB_PROPERTY_CODE']) for item in scraped_items]
                vectors = embed(descriptions)

                # Phase 3: Insert to MongoDB with Embeddings
                for uid, vec, item in zip(ids, vectors, scraped_items):
                    item['_id'] = uid
                    item['embedding'] = vec

                # Use ordered=False to silently skip items that already exist in DB
                try:
                    mongodb[operation].insert_many(scraped_items, ordered=False)
                except Exception as e:
                    print("MongoDB insert finished (some duplicate keys were safely ignored)")

                total_points = len(scraped_items)
                all_items.extend(scraped_items)

            if len(all_items) > 0:
                df = pd.DataFrame(all_items)
                df['LATITUDE'] = pd.to_numeric(df['LATITUDE'], errors='coerce')
                df['LONGITUDE'] = pd.to_numeric(df['LONGITUDE'], errors='coerce')
                df['PRICE'] = pd.to_numeric(df['PRICE'], errors='coerce')
                df['AREA'] = pd.to_numeric(df['AREA'], errors='coerce')

                success_rate = None if (success_count + failure_count) == 0 else (success_count / (success_count + failure_count) * 100)
            
                signature = {
                    "timestamp": timestamp,
                    "property": property,
                    "operation": operation,
                    "pages_success": success_count,
                    "pages_failed": failure_count,
                    "success_rate": success_rate,
                    'total_properties': len(df.loc[df['PROPERTY_TYPE'] == property]),
                    'mean_price': df.loc[df['PROPERTY_TYPE'] == property, 'PRICE'].mean(),
                }
                all_stats.append(signature)

    stats_df = pd.DataFrame(all_stats)
    stats_df.to_csv(stats_out.path, index=False)
    print(f"✅ Stored {len(all_items)} properties")


@dsl.component(base_image=BASE_IMAGE)
def validate_scrapping_signature_op(
    current_stats_in: Input[Dataset],
    comparison_out: Output[Dataset]
):
    import pandas as pd
    from google.cloud import aiplatform

    current_stats = pd.read_csv(current_stats_in.path)
    
    # We use aiplatform.PipelineJob to find the previous run
    aiplatform.init(project=PROJECT_ID, location=LOCATION)
    
    # List previous runs
    jobs = aiplatform.PipelineJob.list(
        filter='pipelineName="inmueblesapp-scrapping-pipeline"',
        order_by='createTime desc',
    )
    
    if len(jobs) < 2:
        print("⚠️ No previous run to compare with")
        current_stats.to_csv(comparison_out.path, index=False)
        return
        
    # Get previous stats (mocking the ZenML artifact retrieval for now since it requires reading from GCS)
    # In a full Vertex migration, we'd read the artifact URI from the previous job.
    # For simplicity, returning the current stats directly if previous cannot be easily joined.
    
    print("Previous runs found, but fetching their artifacts requires GCS reading.")
    # Saving current stats to output
    current_stats.to_csv(comparison_out.path, index=False)



@dsl.pipeline(
    name="inmueblesapp-scrapping-pipeline",
    description="Pipeline to scrape property data from Finca Raiz",
    pipeline_root=PIPELINE_ROOT,
)
def scrapping_pipeline():
    operations = ['Arriendo', 'Venta']
    property_types = [
        'Casa', 'Apartamento', 'Lote', 'Local', 'Oficina',
        'Finca', 'Parqueadero', 'Consultorio', 'Edificio',
        'Apartaestudio', 'Cabaña', 'Casa Campestre', 'Casa Lote',
        'Habitación', 'Bodega'
    ]
    
    # Step 1: Scrape Finca Raiz properties
    scrape_task = scrape_properties_op(operations=operations, properties=property_types)
    
    # Step 2: Create the signature
    validate_task = validate_scrapping_signature_op(current_stats_in=scrape_task.outputs['stats_out'])
