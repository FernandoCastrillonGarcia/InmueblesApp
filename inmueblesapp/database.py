import os
from dotenv import load_dotenv
from qdrant_client import QdrantClient
from typing import Optional
from pymongo import MongoClient

load_dotenv()

class QdrantSingleton:
    _instance: Optional['QdrantSingleton'] = None
    _client: Optional[QdrantClient] = None
    _is_local: Optional[bool] = None
    
    def __new__(cls, local: bool = False) -> 'QdrantSingleton':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, local: bool = False):
        if self._client is None:
            self._is_local = local
            if self._is_local:
                self._client = QdrantClient(url="http://localhost:6333")
            else:
                print(os.getenv('QDRANT_HOST'))
                self._client = QdrantClient(
                    os.getenv('QDRANT_HOST'),
                    api_key=os.getenv('QDRANT_API_KEY')
                )
    
    @property
    def client(self) -> QdrantClient:
        return self._client
    
    @classmethod
    def get_client(cls, local: bool = False) -> QdrantClient:
        instance = cls(local=local)
        return instance.client

class MongoSingleton:
    _instance: Optional['MongoSingleton'] = None
    _client: Optional[MongoClient] = None
    
    def __new__(cls) -> 'MongoSingleton':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if self._client is None:
            mongo_uri = os.getenv("MONGO_URI")
            if not mongo_uri:
                raise ValueError("MONGO_URI environment variable is not set")
            self._client = MongoClient(mongo_uri)
    
    @property
    def client(self) -> MongoClient:
        return self._client
    
    @classmethod
    def get_client(cls) -> MongoClient:
        instance = cls()
        return instance.client

# Usage example
if __name__ == '__main__':
    qdrant_client = QdrantSingleton.get_client()
    print(qdrant_client.get_collections())

    
