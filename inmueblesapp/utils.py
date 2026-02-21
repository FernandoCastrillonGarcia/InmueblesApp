import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import numpy as np
import uuid
from database import QdrantSingleton
from fastembed import TextEmbedding
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

# Module-level singleton: loaded once, reused across all embed() calls
_embedding_model: TextEmbedding | None = None

def _get_embedding_model() -> TextEmbedding:
    """Lazy-load the embedding model once per process."""
    global _embedding_model
    if _embedding_model is None:
        import onnxruntime as ort
        available = ort.get_available_providers()
        providers = ["CUDAExecutionProvider", "CPUExecutionProvider"] if "CUDAExecutionProvider" in available else ["CPUExecutionProvider"]
        _embedding_model = TextEmbedding(
            model_name="nomic-ai/nomic-embed-text-v1.5",
            providers=providers,
            show_progress=False
        )
    return _embedding_model

import streamlit as st
def embed(text: list[str] | str) -> list[list[float]]:
    """Embed text using the singleton FastEmbed model.

    Args:
        text: Single string or list of strings to embed.

    Returns:
        List of embedding vectors as lists of floats.
    """
    if isinstance(text, list):
        input_texts = [preprocess_text(t) for t in text]
    else:
        input_texts = [preprocess_text(text)]
    model = _get_embedding_model()
    
    embeddings = list(model.embed(input_texts, batch_size = 16))
    
    return [e.tolist() for e in embeddings]


def query(text: str, collection_name: str = "Arriendo", payload: dict = None, limit=10, local=True) -> list:
    client = QdrantSingleton(local=local).get_client()
    vector = embed(text)[0]

    search_result = client.query_points(
        collection_name=collection_name,
        query=vector,
        query_filter=Filter(**payload) if payload else None,
        with_payload=True,
        limit=limit,
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

if __name__=='__main__':
    pass