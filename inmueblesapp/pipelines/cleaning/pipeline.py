from zenml import pipeline
import pipelines.cleaning.steps as steps
from pipelines.cleaning.steps import LOCAL

@pipeline
def cleaning_pipeline():
    """
    Pipeline to clean the MongoDB and Qdrant databases.
    
    Steps:
    1. Remove duplicates by SOURCE and WEB_PROPERTY_CODE.
    2. Remove erroneous values (sentinel values, apartment-specific rules).
    3. Cap prices: negatives → 0, above p99 per PROPERTY_TYPE → p99.
    4. Cap numeric fields: BUILT_AREA, AREA, GARAGE, BATHROOMS, ROOMS per PROPERTY_TYPE.
    """
    collections = ["Arriendo", "Venta"]

    #stats = steps.remove_duplicates_by_source()
    erroneous_stats = steps.remove_erroneous_values(
        collections=collections, local=LOCAL,
    )
    price_stats = steps.cap_prices_by_property_type(
        collections=collections, local=LOCAL, after=[erroneous_stats],
    )
    numeric_stats = steps.cap_numeric_fields(
        collections=collections, local=LOCAL, after=[price_stats],
    )

