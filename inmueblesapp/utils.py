import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import numpy as np
import uuid
from database import QdrantSingleton

import ollama
import re
import os
from database import QdrantSingleton
import streamlit as st

from qdrant_client.models import Filter
def preprocess_text(text):
    # Remove \n and replace with spaces
    text = text.replace('\n', ' ')
    # Remove unicode spaces like \u202f
    text = re.sub(r'[\u00A0\u1680\u2000-\u200B\u202F\u205F\u3000]', ' ', text)
    # Replace multiple spaces with single space
    text = re.sub(r'\s+', ' ', text)
    # Strip leading/trailing whitespace
    return text.strip()

def embed(text:list[str] | str)->list[list[float]]:

    if isinstance(text,list):
        input = [preprocess_text(t) for t in text]
    else:
        input = preprocess_text(text)

    # Configure Ollama client with host from environment variable
    
    ollama_host = os.getenv('OLLAMA_HOST', 'https://localhost:11434')
    
    client = ollama.Client(host=ollama_host)
    
    return client.embed(model = 'nomic-embed-text', input = input)['embeddings']


def query(text:str, payload:[dict] = None, limit = 10, local=True)->list:

    client = QdrantSingleton(local=local).get_client()
    
    vector = embed(text)[0]
    
    search_result = client.query_points(
        collection_name="Arriendo",
        query=vector,
        query_filter = Filter(**payload),
        with_payload=True,
        limit=limit
    ).points

    return search_result

def create_uuid_from_string(input_data):
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, str(input_data)))

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