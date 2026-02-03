from zenml import pipeline
import json

import pipelines.training.steps as steps


@pipeline(enable_cache=False)
def price_prediction_pipeline(model_type: str = "xgboost"):
    """Complete price prediction training pipeline"""

    # Load config
    with open('inmueblesapp/pipelines/training/src/config.json', 'r') as f:
        config_dict = json.load(f)

    # Load data
    raw_data = steps.load_data()

    # Step 2-4: Clean (your data_cleaner steps)
    selected_data = steps.drop_columns(raw_data, config_dict)
    filled_data = steps.fill_categorical_features(selected_data, config_dict)
    clean_data = steps.manual_filtering(filled_data)
    
    # Step 5: Feature engineering (your data_preprocessor)
    X_train, X_test, y_train, y_test = steps.split_data(clean_data, config_dict)
    X_preprocessor, y_preprocessor = steps.build_features_pipelines()

    # Un pack Processed data and preprocessing pipelines
    _ = steps.process_data(X_train, X_test, y_train, 
                        X_preprocessor, y_preprocessor)

    X_train_processed, X_test_processed, y_train_processed, y_preprocessor, X_preprocessor = _

    
    # Step 6: Train
    model, metrics = steps.train_model(
        X_train_processed, X_test_processed, y_train_processed, y_test,
        y_preprocessor,
        X_preprocessor,
        X_train, y_train, X_test,
        model_type=model_type
    )
    
    return model, metrics

if __name__ == '__main__':
    price_prediction_pipeline("xgboost")