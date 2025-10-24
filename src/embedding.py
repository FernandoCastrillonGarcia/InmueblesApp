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

