import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
from database import MongoSingleton

st.set_page_config(
    page_title="Monitor de Datos",
    page_icon="📈",
    layout="wide"
)

st.title("📈 Monitor de Scraping y Mercado")

# Connect to DB
try:
    client = MongoSingleton.get_client()
    db = client["inmuebles_db"]
    collection = db["properties"]
    
    # Check connection
    item_count = collection.count_documents({})
    st.success(f"Conectado a MongoDB: {item_count} propiedades")
except Exception as e:
    st.error(f"Error conectando a MongoDB: {e}")
    st.stop()

# --- Load Data ---
@st.cache_data(ttl=600)  # Cache for 10 mins
def load_monitoring_data():
    # Fetch required fields only to optimize
    cursor = collection.find(
        {}, 
        {
            "scraped_at": 1, 
            "PRICE": 1, 
            "AREA": 1, 
            "OPERATION_TYPE": 1, 
            "PROPERTY_TYPE": 1,
            "batch_id": 1
        }
    )
    df = pd.DataFrame(list(cursor))
    print(df)
    
    if not df.empty:
        # Normalize types
        df['PRICE'] = pd.to_numeric(df['PRICE'], errors='coerce')
        df['AREA'] = pd.to_numeric(df['AREA'], errors='coerce')
        df['scraped_at'] = pd.to_datetime(df['scraped_at'])
    return df

with st.spinner("Cargando datos..."):
    df = load_monitoring_data()

if df.empty:
    st.warning("No hay datos en la base de datos.")
    st.stop()

# --- Data Quality ---
st.markdown("### 🛡️ Calidad de Datos")

missing_price = df['PRICE'].isna().sum()
missing_area = df['AREA'].isna().sum()
total = len(df)

col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Total Registros", total)
with col2:
    st.metric("Sin Precio", f"{missing_price} ({missing_price/total:.1%})", delta_color="inverse")
with col3:
    st.metric("Sin Área", f"{missing_area} ({missing_area/total:.1%})", delta_color="inverse")

# Recent Batch Stats
latest_batch = df['batch_id'].max() if 'batch_id' in df.columns else "N/A"
st.caption(f"Último batch de carga: {latest_batch}")

# --- Market Pulse ---
# st.markdown("### 💓 Pulso del Mercado")

# # Aggregate by day and property type
# daily_counts = df.groupby([df['scraped_at'].dt.date, 'PROPERTY_TYPE']).size().reset_index()
# daily_counts.columns = ['Fecha', 'Tipo de Propiedad', 'Nuevas Propiedades']

# fig_pulse = px.line(
#     daily_counts, 
#     x='Fecha', 
#     y='Nuevas Propiedades', 
#     color='Tipo de Propiedad',  # Add color dimension
#     title="Propiedades Scrapeadas por Día y Tipo",
#     markers=True
# )
# st.plotly_chart(fig_pulse, use_container_width=True)

# --- Price Distribution ---
st.markdown("### 💰 Distribución de Precios")

col1, col2 = st.columns(2)
with col1:
    op_filter = st.selectbox("Operación", df['OPERATION_TYPE'].unique())
with col2:
    prop_filter = st.selectbox("Tipo de Propiedad", df['PROPERTY_TYPE'].dropna().unique())

filtered_df = df[
    (df['OPERATION_TYPE'] == op_filter) & 
    (df['PROPERTY_TYPE'] == prop_filter)
]

filtered_df['PRICE_MILLIONS'] = filtered_df['PRICE'] / 1e6

if not filtered_df.empty:
    # Histogram
    fig_hist = px.histogram(
        filtered_df, 
        x="PRICE_MILLIONS", 
        nbins=50, 
        title=f"Distribución de Precios ({op_filter} - {prop_filter})",
        labels={"PRICE": "Precio [Millones COP]"}
    )
    st.plotly_chart(fig_hist, use_container_width=True)
    
    # Box plot for anomalies
    fig_box = px.box(
        filtered_df,
        x="PRICE",
        title=f"Detectando Anomalías ({op_filter} - {prop_filter})",
        points="outliers"
    )
    st.plotly_chart(fig_box, use_container_width=True)
else:
    st.info("No hay datos para los filtros seleccionados.")




