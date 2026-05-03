import os
import json
from kfp import dsl

PROJECT_ID = os.getenv("PROJECT_ID", "inmuebles-app-437-v2")
LOCATION = os.getenv("LOCATION", "us-central1")
PIPELINE_ROOT = f"gs://{PROJECT_ID}-pipeline-roots/inmueblesapp"
BASE_IMAGE = f"us-east1-docker.pkg.dev/{PROJECT_ID}/inmuebles-app/pipeline-runner:latest"

@dsl.component(base_image=BASE_IMAGE)
def train_model_op(model_type: str = "xgboost") -> str:
    import pandas as pd
    import numpy as np
    import mlflow
    import json
    import os
    from glob import glob
    from sklearn.model_selection import train_test_split
    from sklearn.compose import ColumnTransformer, TransformedTargetRegressor
    from sklearn.impute import SimpleImputer
    from sklearn.pipeline import Pipeline
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.preprocessing import StandardScaler, OneHotEncoder, FunctionTransformer, QuantileTransformer
    from xgboost import XGBRegressor
    from lightgbm import LGBMRegressor

    # Import helpers from our new utils directory
    from pipelines.utils import evaluate as ev
    from pipelines.utils import preprocess as pp
    from pipelines.utils import optimize as op
    from backend.database import MongoSingleton

    # === 1. Load Data ===
    try:
        # Initialize MLflow experiment before any logging
        mlflow.set_tracking_uri("file://" + os.path.abspath("mlruns"))
        mlflow.set_experiment("price_prediction_experiment")
        
        client = MongoSingleton(local=False).client
        db = client["inmuebles_db"]
        print("📂 Loading data from MongoDB...")
        
        df_arriendo = pd.DataFrame(list(db["Arriendo"].find({}, {'_id': 0, 'batch_id': 0, 'scraped_at': 0, 'embedding': 0})))
        df_venta = pd.DataFrame(list(db["Venta"].find({}, {'_id': 0, 'batch_id': 0, 'scraped_at': 0, 'embedding': 0})))
        
        df = pd.concat([df_arriendo, df_venta], ignore_index=True)
        
        df['PRICE'] = pd.to_numeric(df['PRICE'], errors='coerce')
        df['AREA'] = pd.to_numeric(df['AREA'], errors='coerce')
        print('a')
        mlflow.log_param("data_source", "mongodb")
        print('b')
    except Exception as e:
        print(f"⚠️ MongoDB load failed: {e}")
        scraped_files = glob('data/raw/properties_*.csv')
        if not scraped_files:
            df = pd.read_csv('data/raw/data_v1.csv', sep=';')
            mlflow.log_param("data_source", "fallback_data_v1")
        else:
            latest_file = max(scraped_files, key=os.path.getctime)
            df = pd.read_csv(latest_file)
            mlflow.log_param("data_source", os.path.basename(latest_file))
    print(1)
    # === 2. Drop Columns ===
    config_path = os.path.join(os.path.dirname(__file__), 'utils/config.json')
    with open(config_path, 'r') as f:
        config_dict = json.load(f)
        
    y_column = config_dict['y_column']
    numeric_features = config_dict['numeric_features']
    categorical_features = config_dict['categorical_features']
    all_features = numeric_features + categorical_features
    df = df.dropna(subset=y_column + all_features)
    df = df.loc[:, y_column + all_features]
    print(2)
    # === 3. Fill Categorical ===
    for c in categorical_features:
        if pd.api.types.is_numeric_dtype(df[c]):
            df[c] = df[c].fillna(999)
        elif pd.api.types.is_object_dtype(df[c]):
            df[c] = df[c].fillna('No se sabe')
    df[categorical_features] = df[categorical_features].astype('category')
    print(3)
    # === 4. Manual Filtering ===
    df = df.loc[df['FLOOR'] != 202]
    df = df.loc[df['STRATUM'] != 101]
    apartment_mask = df['PROPERTY_TYPE'].isin(['Apartamento', 'Apartaestudio'])
    df.loc[apartment_mask] = df.loc[apartment_mask & (df['AREA'] < 400) & (df['AREA'] > 0) & (df['PRICE'] < 20_000_000) & (df['PRICE'] > 100_000)]
    df.dropna(inplace=True)
    print(4)
    # === 5. Split Data ===
    X_train_raw, X_test_raw, y_train_raw, y_test = train_test_split(df[all_features], df[y_column], test_size=0.2, random_state=42)
    print(5)
    # === 6. Build Pipelines ===
    numeric_transformer = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", StandardScaler())
    ])
    categorical_transformer = Pipeline([
        ("encoder", OneHotEncoder(handle_unknown="ignore", sparse_output=False))
    ])
    X_preprocessor = ColumnTransformer(transformers=[
        ("num", numeric_transformer, numeric_features),
        ("cat", categorical_transformer, categorical_features),
    ])
    y_preprocessor = Pipeline([
        ('winsorize', FunctionTransformer(pp.clip_outliers, inverse_func=None, kw_args={'percentile': 95})),
        ("log_transform", FunctionTransformer(np.log1p, inverse_func=np.expm1)),
        ("quantile", QuantileTransformer(output_distribution='normal')),
    ])
    print(6)
    # === 7. Process Data ===
    X_train_processed = X_preprocessor.fit_transform(X_train_raw)
    y_train_processed = y_preprocessor.fit_transform(y_train_raw)
    print(7)
    # === 8. Train Model ===
    objectives = {"xgboost": op.objective_xgboost, "lightgbm": op.objective_lightgbm, "random_forest": op.objective_random_forest}
    models_map = {"xgboost": XGBRegressor, "lightgbm": LGBMRegressor, "random_forest": RandomForestRegressor}
    
    objective_func = objectives[model_type]
    model_class = models_map[model_type]
    
    print(f"🔍 Optimizing {model_type} hyperparameters...")
    best_params = op.optimize_hyperparameters(objective_func, X_train_processed, y_train_processed.ravel(), n_trials=50)
    
    mlflow.log_params(best_params)
    mlflow.log_param("model_type", model_type)
    
    print(f"🏋️ Training {model_type} with best params...")
    regressor = model_class(**best_params)
    full_model = TransformedTargetRegressor(regressor=regressor, transformer=y_preprocessor)
    
    pipeline = Pipeline([
        ('preprocessor', X_preprocessor),
        ('model', full_model)
    ])
    
    pipeline.fit(X_train_raw, y_train_raw)
    y_test_pred = pipeline.predict(X_test_raw)
    
    test_metrics = ev.regression_metrics(y_test.values, y_test_pred, title=f'Test - {model_type}', print_values=False)
    
    mlflow.log_metrics({                                                                                                                                                                                                                                                                                                                 
        "test_mape": test_metrics['MAPE'], "test_mdape": test_metrics['MDAPE'],
        "test_r2": test_metrics['R2'], "test_rmse": test_metrics['RMSE'], "test_mae": test_metrics['MAE']
    })
    
    input_example = X_train_raw.iloc[:5]
    mlflow.sklearn.log_model(pipeline, "model", input_example=input_example)

    model_uri = f"runs:/{mlflow.active_run().info.run_id}/model"
    registered_model = mlflow.register_model(model_uri=model_uri, name="price_prediction_model")

    print(f"✅ Model registered as version {registered_model.version}")
    return json.dumps(test_metrics)


# We import the components from the other files at the top level to avoid KFP nested pipeline errors
from pipelines.scrapping import scrape_properties_op
from pipelines.cleaning import remove_erroneous_values_op, cap_prices_by_property_type_op, cap_numeric_fields_op

@dsl.pipeline(
    name="inmueblesapp-end-to-end-pipeline",
    description="End-to-End Pipeline to scrape, clean, and train property price models",
    pipeline_root=PIPELINE_ROOT,
)
def end_to_end_pipeline(project_id: str = PROJECT_ID, model_type: str = "xgboost"):
    # Step 1: Scrape
    scrape_task = scrape_properties_op(
        operations=['Arriendo', 'Venta'], 
        properties=['Casa', 'Apartamento', 'Lote', 'Local', 'Oficina', 'Finca', 'Parqueadero']
    )
    
    # Step 2: Clean (We string them together using .after() to ensure chronological execution in the cloud)
    erroneous_task = remove_erroneous_values_op(collections=['Arriendo', 'Venta'], local=False).after(scrape_task)
    price_task = cap_prices_by_property_type_op(collections=['Arriendo', 'Venta'], local=False).after(erroneous_task)
    numeric_task = cap_numeric_fields_op(collections=['Arriendo', 'Venta'], local=False).after(price_task)
    
    # Step 3: Train
    train_task = train_model_op(model_type=model_type).after(numeric_task)