from zenml import pipeline
import pipelines.cleaning.steps as steps

@pipeline
def cleaning_pipeline():
    """
    Pipeline to clean the MongoDB database.
    
    Steps:
    1. Remove duplicates by SOURCE and WEB_PROPERTY_CODE.
    2. Cap prices: negatives → 0, above p99 per PROPERTY_TYPE → p99.
    """
    #stats = steps.remove_duplicates_by_source()
    price_stats = steps.cap_prices_by_property_type(
        collections=["Arriendo", "Venta"],
        local=True,
    )

