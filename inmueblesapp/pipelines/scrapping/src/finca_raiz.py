import requests
import os
from qdrant_client.models import PointStruct
from utils import embed, preprocess_text
from utils import create_uuid_from_string
from dotenv import load_dotenv
load_dotenv()

OPERATION_INDEX = {
    'Venta': 1,
    'Arriendo': 2
}
INDEX_OPERATION = {value:key for key, value in OPERATION_INDEX.items()}

PROPERTY_INDEX = {
    'Casa': 1,
    'Apartamento': 2,
    'Lote': 3,
    'Local': 4,
    'Oficina': 5,
    'Finca': 6,
    'Parqueadero': 8,
    'Consultorio': 9,
    'Edificio': 10,
    'Apartaestudio': 14,
    'Cabaña': 15,
    'Casa Campestre': 16,
    'Casa Lote': 17,
    'Habitación': 18,
    'Bodega': 19

}
INDEX_PROPERTY = {value:key for key, value in PROPERTY_INDEX.items()}

ANTIQUITY_INDEX = {
    'Menor a 1 año': 1,
    'De 1 a 8 años':2,
    'De 9 a 15 años': 3,
    'De 16 a 30 años': 4,
    'Más de 30 años': 5
}
INDEX_ANTIQUITY = {value:key for key, value in ANTIQUITY_INDEX.items()}

LOCAL = os.getenv("LOCAL", "true").lower() == "true"

def get_location(query:str)->dict:
    url = "https://search-service.fincaraiz.com.co/api/v1/locations/infofinca-autocomplete"

    payload = {
        "operationName": "Location",
        "variables": {"strSearch": query},
        "query": ""
    }
    headers = {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/json",
        "origin": "https://www.fincaraiz.com.co",
        "priority": "u=1, i",
        "referer": "https://www.fincaraiz.com.co/venta/casas-y-apartamentos/bogota/bogota-dc",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
        "x-origin": "www.fincaraiz.com.co"
    }

    response = requests.request("POST", url, json=payload, headers=headers).json()
    
    return {
        'type': response['data']['searchLocation'][0]['type'],
        'name': response['data']['searchLocation'][0]['name'],
        'id': response['data']['searchLocation'][0]['id']

    }

def get_total_hits(property_type_id, operation_type_id, projects=None, location=None)->dict:
    url =  "https://search-service.fincaraiz.com.co/api/v1/properties/search"

    payload = {
        "variables": {
            "rows": 1,
            "params": {
                "page": 1,
                "order": 2,
                "operation_type_id": operation_type_id,
                "property_type_id": [property_type_id],
                "currencyID": 4,
                "m2Currency": 4,
                "locations": [location]
            },
            "page": 1,
            "source": 10
        },
        "query": ""
    }

    if projects is not None:
        payload["variables"]["params"]["projects"] = projects

    if location is not None:
        payload["variables"]["params"]["locations"] = [location]
    
    headers = {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/json",
        "origin": "https://www.fincaraiz.com.co",
        "priority": "u=1, i",
        "referer": "https://www.fincaraiz.com.co/",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
        "x-origin": "www.fincaraiz.com.co"
    }


    response = requests.request("POST", url, json=payload, headers=headers).json()
    
    return response.get('hits',{'message':'No hay hits'}).get('total',{'message':'No hay total'}).get('value', -1)

def get_total_pages(total_hits:int, rows:int) -> int:
        pages = total_hits // rows
        if total_hits % rows > 0:
            pages += 1
        return pages

def hits_to_points(
    items: list[dict],
    vectors: list[list[float]] | None = None) -> list[PointStruct]:
    """Convert scraped items to Qdrant PointStructs.

    Args:
        items: List of property dicts (DESCRIPTION is popped as side-effect).
        vectors: Pre-computed embeddings. If None, computes them on the fly.

    Returns:
        List of PointStruct ready for Qdrant upsert.
    """
    descriptions = [preprocess_text(item.pop('DESCRIPTION')) for item in items]
    ids = [create_uuid_from_string(item['WEB_PROPERTY_CODE']) for item in items]

    if vectors is None:
        vectors = embed(descriptions)

    return [PointStruct(id=id, vector=vector, payload=item)
            for id, vector, item in zip(ids, vectors, items)]
    
def get_hits(rows, pages, property_type_id, operation_type_id, projects=None, location=None)->list[dict]:

    url =  "https://search-service.fincaraiz.com.co/api/v1/properties/search"

    payload = {
        "variables": {
            "rows": rows,
            "params": {
                "page": pages,
                "order": 2,
                "operation_type_id": operation_type_id,
                "property_type_id": [property_type_id],
                "currencyID": 4,
                "m2Currency": 4,
                "locations": [location]
            },
            "page": pages,
            "source": 10
        },
        "query": ""
    }

    if projects is not None:
        payload["variables"]["params"]["projects"] = projects

    if location is not None:
        payload["variables"]["params"]["locations"] = [location]
    
    headers = {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/json",
        "origin": "https://www.fincaraiz.com.co",
        "priority": "u=1, i",
        "referer": "https://www.fincaraiz.com.co/",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
        "x-origin": "www.fincaraiz.com.co"
    }

    response = requests.request("POST", url, json=payload, headers=headers).json()

    response_list = response['hits']['hits']

    items = []
    for hit in response_list:

        property = hit['_source']['listing']

        item = {
            'SOURCE': 'Finca Raiz',
            'WEB_PROPERTY_CODE': property['id'],
            'PRICE': property['price']['amount'],
            'PRICE_ADMIN_INCLUDED': property['price']['admin_included'],
            'AREA': property['m2'],
            'LATITUDE': property['latitude'],
            'LONGITUDE': property['longitude'],
            'ANTIQUITY': INDEX_ANTIQUITY.get(property['antiquity'], None),
            'CONSTRUCTION_YEAR': property['construction_year'],
            'BUILT_AREA': property['m2Built'],
            'PRIVATE_AREA': property['m2apto'],
            'GARAGE': property['garage'],
            'BATHROOMS': property['bathrooms'],
            'ROOMS': property['rooms'],
            'FLOOR': property['floor'],
            'PROPERTY_TYPE': INDEX_PROPERTY.get(property['property_type_id'],None),
            'OPERATION_TYPE': INDEX_OPERATION.get(property['operation_type_id'],None),
            'STRATUM': property['stratum'],
            'BEDROOMS': property['bedrooms'],
            'DESCRIPTION': property['description'],
            'LINK': 'https://www.fincaraiz.com.co'+property['link'],
            # 'FUNCIONA': True # Si el coso esta en funca raiz, pues en teoria está funcionando
        }

        items.append(item)

    return items
   
