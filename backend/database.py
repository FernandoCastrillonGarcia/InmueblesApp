import os
from dotenv import load_dotenv
from typing import Optional
from pymongo import MongoClient

load_dotenv()

class MongoSingleton:
    _instance: Optional['MongoSingleton'] = None
    _client: Optional[MongoClient] = None
    _is_local: Optional[bool] = None

    def __new__(cls, local: bool = False) -> 'MongoSingleton':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, local: bool = False):
        if self._client is None:
            self._is_local = local
            if self._is_local:
                url = os.getenv("MONGO_LOCAL_URI", "mongodb://localhost:27017")
            else:
                url = os.getenv("MONGO_URI")
                if not url:
                    raise ValueError("MONGO_URI environment variable is not set")
            self._client = MongoClient(url)

    @property
    def client(self) -> MongoClient:
        return self._client

    @classmethod
    def get_client(cls, local: bool = False) -> MongoClient:
        instance = cls(local=local)
        return instance.client

def get_db():
    local = os.getenv("LOCAL", "true").lower() == "true"
    client = MongoSingleton.get_client(local=local)
    return client["inmuebles_db"]
