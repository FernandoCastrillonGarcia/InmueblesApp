from zenml import pipeline
import json
from steps.data_loader import load_data
from steps import data_cleaner as cleaner
from steps import data_preprocessor as pp
from steps import model_trainer as trainer

@pipeline(enable_cache=False)
def price_prediction_pipeline(model_type: str = "xgboost"):
    """Complete price prediction training pipeline"""
    
    # Load data
    raw_data = load_data()

    # Load config
    with open('src/config.json', 'r') as f:
        config_dict = json.load(f)

    # Step 2-4: Clean (your data_cleaner steps)
    selected_data = cleaner.drop_columns(raw_data, config_dict)
    filled_data = cleaner.fill_categorical_features(selected_data, config_dict)
    clean_data = cleaner.manual_filtering(filled_data)
    
    # Step 5: Feature engineering (your data_preprocessor)
    X_train, X_test, y_train, y_test = pp.split_data(clean_data, config_dict)
    X_preprocessor, y_preprocessor = pp.build_features_pipelines()

    # Un pack Processed data and preprocessing pipelines
    _ = pp.process_data(X_train, X_test, y_train, 
                        X_preprocessor, y_preprocessor)

    X_train_processed, X_test_processed, y_train_processed, y_preprocessor = _

    
    # Step 6: Train
    model, metrics = trainer.train_model(
        X_train_processed, X_test_processed, y_train_processed, y_test,
        y_preprocessor,
        model_type=model_type
    )
    
    return model, metrics

if __name__ == '__main__':
    price_prediction_pipeline("xgboost")