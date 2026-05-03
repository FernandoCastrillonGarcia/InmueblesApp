import os
import pandas as pd
import mlflow
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from typing import Optional

router = APIRouter()

# Input Schema
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
    GARAGE: Optional[float] = 0
    STRATUM: int
    BEDROOMS: int
    
    # Categorical features
    ANTIQUITY: str
    PROPERTY_TYPE: str

# In a real app with lifespan events, the model is attached to the app state
# For modularity, we'll expose a function or assume it's attached to request.app.state.model
# Or load it dynamically if None
_MODEL = None

def get_model():
    global _MODEL
    if _MODEL is not None:
        return _MODEL

    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "file:///app/mlruns")
    mlflow.set_tracking_uri(tracking_uri)
    
    try:
        _MODEL = mlflow.sklearn.load_model("models:/price_prediction_model/latest")
    except Exception as e:
        print(f"Failed to load from registry: {e}. Scanning filesystem fallback...")
        base_path = "/app/mlruns"
        if os.path.exists(base_path):
            latest_model_path = None
            latest_time = 0
            for root, dirs, files in os.walk(base_path):
                if "MLmodel" in files and root.endswith("model"):
                    mod_time = os.path.getmtime(root)
                    if mod_time > latest_time:
                        latest_time = mod_time
                        latest_model_path = root
            if latest_model_path:
                _MODEL = mlflow.sklearn.load_model(latest_model_path)
    
    return _MODEL

@router.post("/predict")
def predict_price(property: PropertyInput):
    model = get_model()
    if not model:
        raise HTTPException(status_code=503, detail="Model not loaded or found in mlruns")
    
    data_dict = property.model_dump()
    data = pd.DataFrame([data_dict])
    
    try:
        prediction = model.predict(data)
        price = prediction[0] if hasattr(prediction, '__iter__') else prediction
        return {"predicted_price": float(price)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
