import os
from dotenv import load_dotenv
from qdrant_client import QdrantClient
from typing import Optional
load_dotenv()

class QdrantSingleton:
    _instance: Optional['QdrantSingleton'] = None
    _client: Optional[QdrantClient] = None
    
    def __new__(cls) -> 'QdrantSingleton':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._client is None:
            self._client = QdrantClient(
                os.getenv('QDRANT_HOST'),
                api_key=os.getenv('QDRANT_API_KEY')
            )
    
    @property
    def client(self) -> QdrantClient:
        return self._client
    
    @classmethod
    def get_client(cls) -> QdrantClient:
        instance = cls()
        return instance.client

# Usage example

if __name__ == '__main__':
    qdrant_client = QdrantSingleton.get_client()
    print(qdrant_client.get_collections())