import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import numpy as np

from database import QdrantSingleton

def create_color_palette(n):
    
    forestgreen = np.array([34/255, 139/255, 34/255])
    white = np.array([1, 1, 1])
    
    colors = []
    for i in range(n):
        ratio = i / (n - 1)
        color = forestgreen * (1 - ratio) + white * ratio
        colors.append(mcolors.rgb2hex(color))
    
    return colors


def point_exists_in_collection(point_id: int | str, collection_name: str, local: bool = False) -> bool:
    """
    Check if a point with the given ID exists in the specified Qdrant collection.
    
    Args:
        point_id: The ID of the point to check (can be int or str)
        collection_name: Name of the Qdrant collection
        local: Whether to use local Qdrant instance (default: False)
    
    Returns:
        bool: True if the point exists, False otherwise
    """
    try:
        client = QdrantSingleton.get_client(local=local)
        
        # Try to retrieve the point
        result = client.retrieve(
            collection_name=collection_name,
            ids=[point_id]
        )
        
        # If we get results, the point exists
        return len(result) > 0
        
    except Exception:
        # If any error occurs (collection doesn't exist, connection issues, etc.)
        return False


def points_that_dont_work(collection_name: str, local: bool = False, limit: int = None):
    """
    Scroll through all points in a Qdrant collection that have FUNCIONA=False 
    or don't have the FUNCIONA key in their payload.
    
    Args:
        collection_name: Name of the Qdrant collection
        local: Whether to use local Qdrant instance (default: False)
        limit: Number of points to retrieve per scroll request (default: 100)
    
    Yields:
        Point: Each point that matches the criteria
    """
    try:
        client = QdrantSingleton.get_client(local=local)
        
        offset = None
        while True:
            result = client.scroll(
                collection_name=collection_name,
                limit=limit,
                offset=offset
            )
            
            points, next_offset = result
            
            if not points:
                break
                
            # Filter points that have FUNCIONA=False or don't have FUNCIONA key
            for point in points:
                if 'FUNCIONA' not in point.payload or point.payload.get('FUNCIONA') is False:
                    yield point
            
            offset = next_offset
            if offset is None:
                break
                
    except Exception as e:
        print(f"Error scrolling collection {collection_name}: {e}")
        return