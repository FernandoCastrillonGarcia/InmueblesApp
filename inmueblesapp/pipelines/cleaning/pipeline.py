from zenml import pipeline
import pipelines.cleaning.steps as steps

@pipeline
def cleaning_pipeline():
    """
    Pipeline to clean the MongoDB database.
    
    Steps:
    1. Remove duplicates by SOURCE and WEB_PROPERTY_CODE.
    """
    stats = steps.remove_duplicates_by_source()

