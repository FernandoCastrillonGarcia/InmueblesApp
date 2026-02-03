import streamlit as st

# Page config
st.set_page_config(
    page_title="Inmuebles App",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state='collapsed'
)

# Homepage
st.title("🏠 Inmuebles App")
st.markdown("### Análisis inteligente de propiedades en Bogotá")

# Main features
col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("#### 🔍 Buscar Propiedades")
    st.write("Encuentra propiedades similares usando IA")
    if st.button("Ir a Búsqueda", key="search"):
        st.switch_page("pages/recommendation.py")

with col2:
    st.markdown("#### 💰 Predecir Precio")
    st.write("Estima el precio de una propiedad")
    if st.button("Ir a Predicción", key="predict"):
        st.switch_page("pages/prediction.py")

with col3:
    st.markdown("#### 📊 Explorar Datos")
    st.write("Analiza el mercado inmobiliario")
    if st.button("Ir a Análisis", key="explore"):
        st.switch_page("pages/monitoring.py")
        

# Quick stats
st.markdown("---")
st.markdown("### 📈 Resumen del Mercado")
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Propiedades", "10,245")
with col2:
    st.metric("Precio Promedio", "$450M")
with col3:
    st.metric("Área Promedio", "85 m²")
with col4:
    st.metric("Precisión Modelo", "92%")