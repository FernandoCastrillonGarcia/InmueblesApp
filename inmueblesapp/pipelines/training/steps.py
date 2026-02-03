
from sklearn.preprocessing import StandardScaler, OneHotEncoder, FunctionTransformer, QuantileTransformer
from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer, TransformedTargetRegressor
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestRegressor
from typing import Tuple, Annotated, Dict

from zenml import step
from glob import glob

import numpy as np
import pandas as pd
import mlflow
import json
import os

from xgboost import XGBRegressor
from lightgbm import LGBMRegressor

import pipelines.training.src.evaluate as ev
import pipelines.training.src.preprocess as pp
import pipelines.training.src.optimize as op
from database import MongoSingleton

@step(experiment_tracker="mlflow_tracker")  # Add tracker
def load_data() -> pd.DataFrame:
    """Load most recent scraped property data from MongoDB or fallback to CSV"""
    
    try:
        client = MongoSingleton.get_client()
        db = client["inmuebles_db"]
        collection = db["properties"]
        
        # Check if collection has data
        if collection.count_documents({}) > 0:
            print("📂 Loading data from MongoDB...")
            cursor = collection.find({}, {'_id': 0, 'batch_id': 0, 'scraped_at': 0})
            df = pd.DataFrame(list(cursor))
            
            # Ensure numeric types
            df['PRICE'] = pd.to_numeric(df['PRICE'], errors='coerce')
            df['AREA'] = pd.to_numeric(df['AREA'], errors='coerce')
            
            mlflow.log_param("data_source", "mongodb")
            mlflow.log_metric("training_data_size", len(df))
            print(f"✅ Loaded {len(df)} properties from MongoDB")
            return df
    except Exception as e:
        print(f"⚠️ MongoDB load failed: {e}")

    scraped_files = glob('data/raw/properties_*.csv')
    
    if not scraped_files:
        print("⚠️ No scraped data found, using fallback")
        df = pd.read_csv('data/raw/data_v1.csv', sep=';')
        mlflow.log_param("data_source", "fallback_data_v1")
    else:
        latest_file = max(scraped_files, key=os.path.getctime)
        print(f"📂 Loading: {latest_file}")
        df = pd.read_csv(latest_file)
        
        # Log which file was used
        mlflow.log_param("data_source", os.path.basename(latest_file))
        mlflow.log_param("data_timestamp", latest_file.split('_')[-1].replace('.csv', ''))
    
    mlflow.log_metric("training_data_size", len(df))
    
    print(f"✅ Loaded {len(df)} properties")
    return df

@step(experiment_tracker = "mlflow_tracker")
def drop_columns(df_raw:pd.DataFrame, config_dict:dict
) -> Annotated[pd.DataFrame, "Target and Features"]:
    """
    Loads the config of the features and drop unnecesary columns. Only keeps features and target.
    """ 

    y_column = config_dict['y_column']
    numeric_features = config_dict['numeric_features']
    categorical_features = config_dict['categorical_features']
    all_features = numeric_features + categorical_features
    
    # Use y and X columns only (Delete Indexes and Ids)
    df = df_raw.loc[:,y_column + all_features]

    return df

@step(experiment_tracker = "mlflow_tracker")
def fill_categorical_features(df:pd.DataFrame, config_dict:dict
) -> Annotated[pd.DataFrame, "Filled Categorical Values"]:
    """
    Fill Null values from categorical data wih 'No se sabe' if it is string and 999 if it is numerical
    """
    categorical_features = config_dict['categorical_features']

    # Categorical features can"t have NaN
    for c in categorical_features:

        # Default Value for Nan numeric
        if pd.api.types.is_numeric_dtype(df[c]):
            df[c] = df[c].fillna(999)

        # Devault Value for Nan String
        elif pd.api.types.is_object_dtype(df[c]):
            df[c] = df[c].fillna('No se sabe')

    df[categorical_features] = df[categorical_features].astype('category')

    return df

@step(experiment_tracker = "mlflow_tracker")
def manual_filtering(df:pd.DataFrame
) -> Annotated[pd.DataFrame, "Dropped useless rows"]:

    old_len = len(df)

    # Drop erroneous data
    df = df.loc[df['FLOOR'] != 202]
    df = df.loc[df['STRATUM'] != 101]

    # Apply area and price filters only for Apartamento and Apartaestudio
    apartment_mask = df['PROPERTY_TYPE'].isin(['Apartamento', 'Apartaestudio'])
    df.loc[apartment_mask] = df.loc[apartment_mask
                                    & (df['AREA'] < 400)
                                    & (df['AREA'] > 0)
                                    & (df['PRICE'] < 20_000_000)
                                    & (df['PRICE'] > 100_000)]

    # Drop remaining NaN
    df.dropna(inplace=True)

    new_len=len(df)

    # TODO: Cambiar a Logger
    print(f"Se removieron {old_len - new_len:,} observacion. un total del {(old_len - new_len) * 100/old_len:,.0f}% de la muestra original")

    return df


@step
def split_data(df:pd.DataFrame, config_dict:dict
)->Tuple[
    Annotated[pd.DataFrame, "X_train"],
    Annotated[pd.DataFrame, "X_test"],
    Annotated[pd.DataFrame, "y_train"],
    Annotated[pd.DataFrame, "y_test"],
]:

    y_column = config_dict['y_column']
    numeric_features = config_dict['numeric_features']
    categorical_features = config_dict['categorical_features']
    all_features = numeric_features + categorical_features

    # Split the data
    X_train, X_test, y_train, y_test = train_test_split(
        df[all_features],
        df[y_column],
        test_size=0.2,
        random_state=42
    )

    return X_train, X_test, y_train, y_test

@step
def build_features_pipelines(
) -> Tuple[
    Annotated[ColumnTransformer, "X_preprocessor"],
    Annotated[Pipeline, "y_preprocessor"]
]:
    """Preprocess data: split, transform, scale"""

        # Load config
    with open('inmueblesapp/pipelines/training/src/config.json', 'r') as f:
        config_dict = json.load(f)

    numeric_features = config_dict['numeric_features']
    categorical_features = config_dict['categorical_features']

    # DELETE PIPELINE
    cutter = Pipeline([
        ('dropna', pp.DropNullColumns()),
        ('trimmer', pp.TrimmColumns(['AREA','PRICE'], tail = 'upper')),  
    ])

    numeric_transformer = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        # ("log_transform", FunctionTransformer(np.log1p, validate=True)),
        ("scaler", StandardScaler())
    ])

    categorical_transformer = Pipeline([
        ("encoder", OneHotEncoder(handle_unknown="ignore", sparse_output=False))
    ])

    X_preprocessor = ColumnTransformer(
        transformers=[
            ("num", numeric_transformer, numeric_features),
            ("cat", categorical_transformer, categorical_features),
        ]
    )

    y_preprocessor = Pipeline([
        ('winsorize',FunctionTransformer(pp.clip_outliers, inverse_func=None, kw_args={'percentile': 95})),
        ("log_transform", FunctionTransformer(np.log1p, inverse_func = np.expm1)),
        ("quantile", QuantileTransformer(output_distribution='normal')),
        #("scaler", StandardScaler())
    ])

    return X_preprocessor, y_preprocessor


@step
def process_data(X_train:pd.DataFrame, X_test:pd.DataFrame, y_train:pd.DataFrame, 
                X_preprocessor:ColumnTransformer, y_preprocessor:Pipeline
)->Tuple[
    Annotated[np.ndarray, "X Train processed"],
    Annotated[np.ndarray, "X test processed"],
    Annotated[np.ndarray, "y train processed"],
    Annotated[Pipeline, "y preprocessor fitted"],
    Annotated[ColumnTransformer, "X preprocessor fitted"]
]:

    X_train_processed = X_preprocessor.fit_transform(X_train)
    y_train_processed = y_preprocessor.fit_transform(y_train)
    X_test_processed = X_preprocessor.transform(X_test)

    # TODO: Cambiar a Logger
    print(f"✅ Preprocessed: X_train={X_train_processed.shape}, X_test={X_test_processed.shape}")

    return (X_train_processed, X_test_processed, y_train_processed, y_preprocessor, X_preprocessor)


@step(experiment_tracker="mlflow_tracker")
def train_model(
    X_train:np.ndarray,
    X_test:np.ndarray,
    y_train:np.ndarray,
    y_test:pd.DataFrame,
    y_preprocessor:Pipeline,
    X_preprocessor:ColumnTransformer,
    X_train_raw:pd.DataFrame,
    y_train_raw:pd.DataFrame,
    X_test_raw:pd.DataFrame,
    model_type: str = "xgboost"
) -> Tuple[Annotated[Pipeline, "model"], Annotated[Dict, "metrics"]]:
    """Train model with Optuna + MLflow tracking"""
    

    # Select objective function
    objectives = {
        "xgboost": op.objective_xgboost,
        "lightgbm": op.objective_lightgbm,
        "random_forest": op.objective_random_forest
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
    best_params = op.optimize_hyperparameters(objective_func, X_train, y_train.ravel())
    
    # Log best params to MLflow
    mlflow.log_params(best_params)
    mlflow.log_param("model_type", model_type)
    
    # Train final model
    print(f"🏋️ Training {model_type} with best params...")
    
    # Create Full Pipeline
    regressor = model_class(**best_params)
    
    # Wrap regressor to handle target transformation automatically
    full_model = TransformedTargetRegressor(regressor=regressor, transformer=y_preprocessor)
    
    pipeline = Pipeline([
        ('preprocessor', X_preprocessor),
        ('model', full_model)
    ])
    
    # Fit pipeline on raw data
    # y_train_raw needs to be passed correctly
    pipeline.fit(X_train_raw, y_train_raw)
    
    # Evaluate using the pipeline
    y_test_pred = pipeline.predict(X_test_raw)
    
    # Calculate metrics
    # Ensure y_test is in correct format (it is likely a DataFrame from split_data)
    test_metrics = ev.regression_metrics(y_test.values, y_test_pred, title=f'Test - {model_type}', print_values=False)
    
    # Log metrics to MLflow
    mlflow.log_metrics({                                                                                                                                                                                                                                                                                                                 
        "test_mape": test_metrics['MAPE'],
        "test_mdape": test_metrics['MDAPE'],
        "test_r2": test_metrics['R2'],
        "test_rmse": test_metrics['RMSE'],
        "test_mae": test_metrics['MAE']
    })
    
    # Log model with input example
    input_example = X_train_raw.iloc[:5]  # Use first 5 samples as example
    mlflow.sklearn.log_model(
        pipeline, 
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
    
    return pipeline, test_metrics


