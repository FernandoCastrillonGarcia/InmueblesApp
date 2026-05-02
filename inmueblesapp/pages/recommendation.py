import streamlit as st
import folium
from streamlit_folium import st_folium
import os
from utils import query
from streamlit import session_state as ss
from database import QdrantSingleton, MongoSingleton
from utils import create_color_palette
from qdrant_client.models import FieldCondition, MatchValue, Range, PayloadSchemaType
from pymongo import ASCENDING, DESCENDING

st.set_page_config(
    page_title="Recomendaciones",
    page_icon="🔍",
    layout="centered"
)


LOCAL = os.getenv("LOCAL", "true").lower() == "true"
# ================ Extracción de constantes ======================
qdrant_client = QdrantSingleton(local=LOCAL).get_client()
mongo_client = MongoSingleton(local=LOCAL).get_client()
mongo_db = mongo_client["inmuebles_db"]


property_types_list = [
    'Casa','Apartamento','Lote','Local','Oficina','Finca','Parqueadero',
    'Consultorio','Edificio','Apartaestudio','Cabaña', 'Casa Campestre',
    'Casa Lote','Habitación','Bodega'
]

operation_types_list = ["Arriendo", "Venta"]

# Header
st.title("🔍 Buscar Propiedades Similares")

# Navigation menu (sidebar)
with st.sidebar:
    st.header("🏠 Inmuebles App")
    st.divider()
    
    # Navigation buttons
    if st.button("🏠 Inicio"):
        st.switch_page("app.py")
    if st.button("💰 Predecir Precio"):
        st.switch_page("pages/prediction.py")
    if st.button("📊 Explorar Datos"):
        st.switch_page("pages/monitoring.py")
    
    st.divider()
  

st.header("Busca tu propiedad")
c1,c2,c3 = st.columns(3)
operation = c1.selectbox("Operación", operation_types_list)
property_type = c2.selectbox("Tipo", property_types_list)
max_results = c3.number_input("Máximo resultados", 5, 50, 10)

@st.cache_data(ttl=300)
def get_field_bounds(collection_name: str, property_type: str, field: str) -> tuple[int, int]:
    """Get min/max of a numeric field from MongoDB, filtered by PROPERTY_TYPE."""
    query_filter = {"PROPERTY_TYPE": property_type, field: {"$exists": True, "$type": "number"}}
    st.write(query_filter)
    min_doc = mongo_db[collection_name].find_one(query_filter, {field: 1}, sort=[(field, ASCENDING)])
    max_doc = mongo_db[collection_name].find_one(query_filter, {field: 1}, sort=[(field, DESCENDING)])
    if min_doc and max_doc:
        return int(min_doc[field]), int(max_doc[field])
    return 0, 1  # fallback

with st.expander('Filtros adicionales'):

    # Filtros generales
    keys = ["BUILT_AREA", "GARAGE", "BATHROOMS", "ROOMS"]
    labels = ["Área construida en m²", "Parqueaderos", "Número de Baños", "Número de Habitaciones"]
    schemas = [PayloadSchemaType.FLOAT] + ([PayloadSchemaType.INTEGER] * 3)
    steps = [10, 1, 1, 1]

    ranges = []
    for key, label, step_val in zip(keys, labels, steps):
        lo, hi = get_field_bounds(operation, property_type, key)
        ranges.append(st.slider(label, lo, max(hi, lo + 1), (lo, max(hi, lo + 1)), step=step_val))

    must_condition = []
    for range_tuple, key, schema in zip(ranges, keys, schemas):
        must_condition.append(FieldCondition(key=key, range=Range(gte=range_tuple[0], lte=range_tuple[1])))

        qdrant_client.create_payload_index(
            collection_name=operation,
            field_name=key,
            field_schema=schema
        )

    n_floor = st.number_input("Ubicación del piso", 0, 30, 3, step=1)


    if "query_payload" not in ss:
        ss.query_payload = {}
    ss.query_payload = {
        'must':must_condition
    }


ss.search_text = st.text_area(
    "Escribe aquí...",
    placeholder="Ej: Apartamento moderno con balcón, cerca del metro, zona segura...",
    height=150
)

search_button = st.button("🔍 Buscar Propiedades", type="primary", use_container_width=True)    
# Results summary (placeholder)
if search_button and ss.search_text:
    
    ss.points = query(ss.search_text, collection_name=operation, limit=max_results, local=LOCAL, payload=ss.query_payload)
    
st.markdown("### Mapa de Resultados")    
# Create folium map (Bogotá center)
m = folium.Map(
    location=[4.6097, -74.0817],
    
    zoom_start=11,
    tiles="OpenStreetMap"
)

if 'points' not in ss:
    ss.points = []

for i, point in enumerate(ss.points):
    
    # Asegurar que la URL tenga protocolo
    link = point.payload.get('LINK', '#')
    if link and not link.startswith(('http://', 'https://')):
        link = f"https://{link}"

    folium.Marker(
        location=[point.payload['LATITUDE'], point.payload['LONGITUDE']],
        popup=folium.Popup(
            f"<b>#{i+1}</b><br>" # TODO: cambiar por el titulo
            f"<b>Precio:</b> ${point.payload.get('PRICE', 'N/A'):,}<br>"
            f"<b>Área:</b> {point.payload.get('AREA', 'N/A')} m²<br>"
            f"<b>Habitaciones:</b> {point.payload.get('ROOMS', 'N/A')}<br>"
            f"<b>Enlace:</b> <a href='{link}' target='_blank'>Ver propiedad</a>",
            max_width=200
        ),
        tooltip=f"#{i+1} - ${point.payload.get('PRICE', 'N/A'):,}",
        icon=folium.DivIcon(
            icon_size=(30, 30),
            icon_anchor=(15, 15),
            html=f'<div style="background-color:#4285F4; color:white; border-radius:50%; '
                 f'width:30px; height:30px; display:flex; align-items:center; '
                 f'justify-content:center; font-weight:bold; font-size:14px; '
                 f'border:2px solid white; box-shadow:0 2px 4px rgba(0,0,0,0.3);">'
                 f'{i+1}</div>'
        )
    ).add_to(m)
# Display map
map_data = st_folium(m, width=700, height=500, returned_objects=[])
