from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from backend.utils import embed
from backend.database import get_db

router = APIRouter()

class RecommendRequest(BaseModel):
    query: str
    operation_type: str = "Arriendo" # e.g. "Arriendo" or "Venta"
    property_type: Optional[str] = None
    min_price: Optional[float] = None
    max_price: Optional[float] = None
    min_area: Optional[float] = None
    limit: int = 10

@router.post("/recommend")
def recommend_properties(req: RecommendRequest):
    db = get_db()
    collection = db[req.operation_type]
    
    try:
        # Generate vector for text query
        vector = embed(req.query)[0]
        
        # Build filter conditions
        filter_conditions = {}
        if req.property_type:
            filter_conditions["PROPERTY_TYPE"] = req.property_type
        
        price_filter = {}
        if req.min_price is not None:
            price_filter["$gte"] = req.min_price
        if req.max_price is not None:
            price_filter["$lte"] = req.max_price
        if price_filter:
            filter_conditions["PRICE"] = price_filter
            
        if req.min_area is not None:
            filter_conditions["AREA"] = {"$gte": req.min_area}
            
        # MongoDB Atlas Vector Search Pipeline
        pipeline = [
            {
                "$vectorSearch": {
                    "index": "vector_index", # Name of the index created in Atlas
                    "path": "embedding", # Field containing the vector
                    "queryVector": vector,
                    "numCandidates": req.limit * 10,
                    "limit": req.limit,
                    "filter": filter_conditions if filter_conditions else None
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "score": {"$meta": "vectorSearchScore"},
                    "PRICE": 1,
                    "AREA": 1,
                    "ROOMS": 1,
                    "BATHROOMS": 1,
                    "LATITUDE": 1,
                    "LONGITUDE": 1,
                    "LINK": 1,
                    "PROPERTY_TYPE": 1,
                    "DESCRIPTION": 1
                }
            }
        ]
        
        results = list(collection.aggregate(pipeline))
        return {"results": results}
        
    except Exception as e:
        # Fallback if vector index is not ready or failed (local dev without Atlas)
        print(f"Vector search failed: {e}. Falling back to standard search.")
        fallback_query = filter_conditions
        results = list(collection.find(fallback_query, {"_id": 0}).limit(req.limit))
        return {"results": results, "warning": "Vector search failed, using standard search."}
