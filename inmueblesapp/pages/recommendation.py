import streamlit as st
import folium
from streamlit_folium import st_folium
from embedding import query
from streamlit import session_state as ss
from database import QdrantSingleton
from utils import create_color_palette
from qdrant_client.models import FieldCondition, MatchValue, Range

st.set_page_config(
    page_title="Recomendaciones",
    page_icon="🔍",
    layout="centered"
)

LOCAL = False
# ================ Extracción de constantes ======================
property_types_list = ["Apartamento", "Apartaestudio", "Casa"]
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
        st.switch_page("pages/analytics.py")
    
    st.divider()
    
    # Search filters
    st.header("Filtros")
    operation = st.selectbox("Operación", operation_types_list)
    property_type = st.selectbox("Tipo", property_types_list)
    max_results = st.slider("Máximo resultados", 5, 50, 10)

    # Filtros generales
    keys = ["BUILT_AREA", "GARAGE", "BATHROOMS", "ROOMS"]
    from qdrant_client.models import PayloadSchemaType
    schemas = [PayloadSchemaType.FLOAT] + ([PayloadSchemaType.INTEGER]*3)
    ranges = []
    with st.expander("Generales"):
        ranges.append(st.slider("Area construida en m²", 0, 1200, (30, 60),step=10))
        ranges.append(st.slider("Número de habitaciones", 0, 10, (2,3),step=1))
        ranges.append(st.slider("Número de Baños", 0, 10, (2,3),step=1))
        ranges.append(st.slider("Número de Parqueaderos", 0, 5, (2,3),step=1))
    
    must_condition = []
    for range_tuple, key, schema in zip(ranges, keys, schemas):
        must_condition.append(FieldCondition(key=key, range=Range(gte=range_tuple[0], lte=range_tuple[1])))
        client = QdrantSingleton(local=LOCAL).get_client()
        # client.create_payload_index(
        #     collection_name="Arriendo",
        #     field_name=key,
        #     field_schema=schema
        # )


    n_floor = st.number_input("Ubicación del piso", 0, 30, 3,step=1)


    if "query_payload" not in ss:
        ss.query_payload = {}
    ss.query_payload = {
        'must':must_condition
    }


st.markdown("### Describe tu propiedad ideal")

ss.search_text = st.text_area(
    "Escribe aquí...",
    placeholder="Ej: Apartamento moderno con balcón, cerca del metro, zona segura...",
    height=150
)

search_button = st.button("🔍 Buscar Propiedades", type="primary", use_container_width=True)    
# Results summary (placeholder)
if search_button and ss.search_text:

    
    ss.points = query(ss.search_text, payload=ss.query_payload, limit=max_results,local=LOCAL)

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
            f"<b>Precio:</b> ${point.payload.get('PRICE', 'N/A'):,}<br>"
            f"<b>Área:</b> {point.payload.get('AREA', 'N/A')} m²<br>"
            f"<b>Habitaciones:</b> {point.payload.get('ROOMS', 'N/A')}<br>"
            f"<b>Enlace:</b> <a href='{link}' target='_blank'>Ver propiedad</a>",
            max_width=200
        ),
        tooltip=f"${point.payload.get('PRICE', 'N/A'):,}",
        icon=folium.Icon(color='blue',icon_color='white', icon='home', prefix='fa')
    ).add_to(m)
# Display map
map_data = st_folium(m, width=700, height=500, returned_objects=[])
