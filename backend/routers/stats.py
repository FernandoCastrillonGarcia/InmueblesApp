from fastapi import APIRouter
from database import get_db

router = APIRouter()

@router.get("/stats")
def get_market_stats():
    db = get_db()
    stats = {}
    
    for op in ["Arriendo", "Venta"]:
        collection = db[op]
        try:
            total = collection.count_documents({})
            avg_pipeline = [
                {"$group": {
                    "_id": None,
                    "avg_price": {"$avg": "$PRICE"},
                    "avg_area": {"$avg": "$AREA"}
                }}
            ]
            agg = list(collection.aggregate(avg_pipeline))
            if agg:
                avg_price = agg[0].get("avg_price", 0)
                avg_area = agg[0].get("avg_area", 0)
            else:
                avg_price = 0
                avg_area = 0
                
            stats[op] = {
                "total_properties": total,
                "avg_price": avg_price,
                "avg_area": avg_area
            }
        except Exception as e:
            stats[op] = {"error": str(e)}
            
    return stats
