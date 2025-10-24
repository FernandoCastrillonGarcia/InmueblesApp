from zenml import step
from zenml.integrations.mlflow.experiment_trackers import MLFlowExperimentTracker
import mlflow
import numpy as np
import pandas as pd
from typing import Dict, Tuple, Annotated
from sklearn.pipeline import Pipeline

from xgboost import XGBRegressor
from lightgbm import LGBMRegressor
from sklearn.ensemble import RandomForestRegressor

from training import optimize_hyperparameters, objective_xgboost, objective_lightgbm, objective_random_forest
from evaluate import regression_metrics

import mlflow
from mlflow.tracking import MlflowClient

@step(experiment_tracker="mlflow_tracker")
def train_model(
    X_train:np.ndarray,
    X_test:np.ndarray,
    y_train:np.ndarray,
    y_test:pd.DataFrame,
    y_preprocessor:Pipeline,
    model_type: str = "xgboost"
) -> Tuple[Annotated[object, "model"], Annotated[Dict, "metrics"]]:
    """Train model with Optuna + MLflow tracking"""
    

    # Select objective function
    objectives = {
        "xgboost": objective_xgboost,
        "lightgbm": objective_lightgbm,
        "random_forest": objective_random_forest
    }
    
    models_map = {
        "xgboost": XGBRegressor,
        "lightgbm": LGBMRegressor,
        "random_forest": RandomForestRegressor
    }
    
    objective_func = objectives[model_type]
    model_class = models_map[model_type]
    
    # Optimize hyperparameters (Optuna)
    print(f"🔍 Optimizing {model_type} hyperparameters...")
    best_params = optimize_hyperparameters(objective_func, X_train, y_train.ravel())
    
    # Log best params to MLflow
    mlflow.log_params(best_params)
    mlflow.log_param("model_type", model_type)
    
    # Train final model
    print(f"🏋️ Training {model_type} with best params...")
    model = model_class(**best_params)
    model.fit(X_train, y_train.ravel())
    
    # Evaluate
    y_train_raw = model.predict(X_train)
    y_test_raw = model.predict(X_test)
    
    # Inverse transform
    # y_train_orig = y_preprocessor.inverse_transform(y_train.reshape(-1, 1)).flatten()
    # y_train_pred = y_preprocessor.inverse_transform(y_train_raw.reshape(-1, 1)).flatten()
    y_test_pred = y_preprocessor.inverse_transform(y_test_raw.reshape(-1, 1)).flatten()
    
    # Calculate metrics
    test_metrics = regression_metrics(y_test.values, y_test_pred, title=f'Test - {model_type}', print_values=False)
    
    # Log metrics to MLflow
    mlflow.log_metrics({                                                                                                                                                                                                                                                                                                                 
        "test_mape": test_metrics['MAPE'],
        "test_mdape": test_metrics['MDAPE'],
        "test_r2": test_metrics['R2'],
        "test_rmse": test_metrics['RMSE'],
        "test_mae": test_metrics['MAE']
    })
    
    # Log model with input example
    input_example = X_train[:5]  # Use first 5 samples as example
    mlflow.sklearn.log_model(
        model, 
        "model",
        input_example=input_example
    )

    # Register the model
    model_uri = f"runs:/{mlflow.active_run().info.run_id}/model"
    registered_model = mlflow.register_model(
        model_uri=model_uri,
        name="price_prediction_model"
    )

    print(f"✅ Model registered as version {registered_model.version}")
    
    print(f"✅ {model_type} trained - Test MDAPE: {test_metrics['MDAPE']*100:.1f}%")
    
    return model, test_metrics

@step
def compare_models() -> Dict:
    """Compare all model runs and return best"""
    client = MlflowClient()
    
    # Get all runs from experiment
    experiment = client.get_experiment_by_name("price_prediction_pipeline")
    runs = client.search_runs(experiment.experiment_id)
    
    # Find best by MDAPE
    best_run = min(runs, key=lambda r: r.data.metrics.get("test_mdape", 1.0))
    
    print(f"🏆 Best model: {best_run.data.params['model_type']} with MDAPE: {best_run.data.metrics['test_mdape']}")
    
    return {
        "best_model": best_run.data.params['model_type'],
        "best_mdape": best_run.data.metrics['test_mdape']
    }
