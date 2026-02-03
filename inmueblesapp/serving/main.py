import os
import mlflow
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional

app = FastAPI(title="Inmuebles Price Prediction API")

# Define Input Schema based on config.json
class PropertyInput(BaseModel):
    # Numeric features
    AREA: float
    BUILT_AREA: Optional[float] = 0
    PRIVATE_AREA: Optional[float] = 0
    LATITUDE: float
    LONGITUDE: float
    FLOOR: Optional[float] = 0
    ROOMS: int
    BATHROOMS: int
    GARAGE: Optional[float] = 0  # In steps.py it was int, but float is safer for NaNs treated as numeric
    STRATUM: int
    BEDROOMS: int
    
    # Categorical features
    ANTIQUITY: str
    PROPERTY_TYPE: str

# Global model variable
model = None

@app.on_event("startup")
def load_model():
    global model
    try:
        tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "file:///mlruns")
        
        # Detect nested mlruns structure (ZenML artifact store artifact)
        base_path = "/app/mlruns"
        if os.path.exists(os.path.join(base_path, "mlruns")):
            print("📂 Detected nested mlruns directory, adjusting tracking URI...")
            # If we are in nested mode, the 'models' registry is likely deeper or non-existent here
            # But we can try pointing to the nested one
            base_path = os.path.join(base_path, "mlruns")
            tracking_uri = f"file://{base_path}"
            
        print(f"🔗 Using Tracking URI: {tracking_uri}")
        mlflow.set_tracking_uri(tracking_uri)
        
        model_name = "price_prediction_model"
        client = mlflow.MlflowClient()
        
        # Search for latest version
        # Note: In a file-based registry, this works if the DB is set up or using default
        # If simple file store, search_model_versions might be limited.
        # We try to load "models:/price_prediction_model/latest"
        
        # For robustness in this setup:
        # If registry lookup fails, we iterate runs?
        # Let's try standard load first.
        try:
            # 1. Try loading from Registry (if available)
            model = mlflow.sklearn.load_model(f"models:/{model_name}/latest")
            print("✅ Model loaded via Registry")
        except Exception as reg_error:
            print(f"⚠️ Registry load failed: {reg_error}")
            
            # 2. Fallback: Search experiments for latest successful run
            try:
                # Set tracking URI to local file if not set
                if not mlflow.get_tracking_uri():
                    mlflow.set_tracking_uri("file:///app/mlruns")
                
                # Find experiment
                experiment = mlflow.get_experiment_by_name("price_prediction_pipeline")
                if experiment:
                    # Search for successful runs
                    runs = mlflow.search_runs(
                        experiment_ids=[experiment.experiment_id],
                        filter_string="status = 'FINISHED'",
                        order_by=["start_time DESC"],
                        max_results=1
                    )
                    
                    if not runs.empty:
                        run_id = runs.iloc[0].run_id
                        artifact_path = f"runs:/{run_id}/model"
                        print(f"📂 Loading model from run: {run_id}")
                        model = mlflow.sklearn.load_model(artifact_path)
            except Exception as exp_error:
                print(f"⚠️ Experiment search failed: {exp_error}")

        # 3. Final Fallback: Manual filesystem scan (if SQLite db is missing/locked)
        if model is None:
             print("🔍 Scanning local mlruns for artifacts...")
             # This is a bit hacky but works when DB is completely broken in container
             # We assume standard structure: mlruns/<exp_id>/<run_id>/artifacts/model
             base_path = "/app/mlruns"
             if os.path.exists(base_path):
                 # Find latest modified 'model' directory
                 latest_model_path = None
                 latest_time = 0
                 
                 for root, dirs, files in os.walk(base_path):
                     if "MLmodel" in files and root.endswith("model"):
                         mod_time = os.path.getmtime(root)
                         if mod_time > latest_time:
                             latest_time = mod_time
                             latest_model_path = root
                 
                 if latest_model_path:
                     print(f"📂 Found artifact on disk: {latest_model_path}")
                     model = mlflow.sklearn.load_model(latest_model_path)

        if model:
            print("✅ Model loaded successfully")
        else:
            print("❌ Model could not be loaded. Please ensure 'mlruns' is mounted and contains a trained model.")

    except Exception as e:
        print(f"⚠️ Error loading model: {e}")

@app.post("/predict")
def predict_price(property: PropertyInput):
    if not model:
        raise HTTPException(status_code=503, detail="Model not loaded")
    
    # Convert to DataFrame
    data_dict = property.model_dump() # Pydantic v2
    data = pd.DataFrame([data_dict])
    
    try:
        # Prediction
        # The pipeline handles preprocessing and target inverse transform (via TTR)
        prediction = model.predict(data)
        
        # Ensure scalar
        price = prediction[0] if hasattr(prediction, '__iter__') else prediction
        
        return {"predicted_price": float(price)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
def health():
    return {"status": "ok", "model_loaded": model is not None}


