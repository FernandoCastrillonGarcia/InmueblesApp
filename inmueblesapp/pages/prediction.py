import streamlit as st
import requests
import json
import os

st.set_page_config(
    page_title="Predicción de Precios",
    page_icon="💰",
    layout="centered"
)

# API Configuration
API_URL = os.getenv("API_URL", "http://localhost:8000")

st.title("💰 Predicción de Precios")
st.markdown("Ingresa los detalles de la propiedad para estimar su precio.")

# --- Input Form ---
with st.form("prediction_form"):
    
    st.markdown("#### 📍 Ubicación y Características Básicas")
    
    col1, col2 = st.columns(2)
    with col1:
        property_type = st.selectbox(
            "Tipo de Propiedad", 
            ["Apartamento", "Apartaestudio", "Casa", "Oficina", "Local"]
        )
        antiquity = st.selectbox(
            "Antigüedad", 
            ["Menos de 1 año", "1 a 8 años", "9 a 15 años", "16 a 30 años", "Más de 30 años"]
        )
        
    with col2:
        stratum = st.selectbox("Estrato", [1, 2, 3, 4, 5, 6], index=3)
        floor = st.number_input("Piso", min_value=1, max_value=50, value=3)

    st.markdown("#### 📐 Áreas y Distribución")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        area = st.number_input("Área Total (m²)", min_value=10.0, max_value=1000.0, value=60.0)
        built_area = st.number_input("Área Construida (m²)", min_value=0.0, max_value=1000.0, value=60.0)
    with col2:
        rooms = st.number_input("Habitaciones", min_value=0, max_value=10, value=2)
        bedrooms = st.number_input("Dormitorios", min_value=0, max_value=10, value=2)
    with col3:
        bathrooms = st.number_input("Baños", min_value=0, max_value=10, value=2)
        garage = st.number_input("Parqueaderos", min_value=0, max_value=5, value=1)
        
    private_area = st.number_input("Área Privada (m²)", min_value=0.0, max_value=1000.0, value=area)

    st.markdown("#### 🗺️ Coordenadas (Opcional)")
    st.caption("Si no las conoces, usa el mapa o valores aproximados de Bogotá")
    
    col1, col2 = st.columns(2)
    with col1:
        latitude = st.number_input("Latitud", value=4.6097, format="%.4f")
    with col2:
        longitude = st.number_input("Longitud", value=-74.0817, format="%.4f")
    
    submitted = st.form_submit_button("🔮 Predecir Precio", type="primary", use_container_width=True)

# --- Logic ---
if submitted:
    
    # Construct payload matching API schema
    payload = {
        "AREA": float(area),
        "BUILT_AREA": float(built_area),
        "PRIVATE_AREA": float(private_area),
        "LATITUDE": float(latitude),
        "LONGITUDE": float(longitude),
        "FLOOR": float(floor),
        "ROOMS": int(rooms),
        "BATHROOMS": int(bathrooms),
        "GARAGE": float(garage),
        "STRATUM": int(stratum),
        "BEDROOMS": int(bedrooms),
        "ANTIQUITY": antiquity,
        "PROPERTY_TYPE": property_type
    }
    
    try:
        with st.spinner("Consultando oráculo inmobiliario..."):
            # Call API
            response = requests.post(f"{API_URL}/predict", json=payload)
            
            if response.status_code == 200:
                result = response.json()
                predicted_price = result.get("predicted_price", 0)
                
                st.success("¡Predicción Exitosa!")
                
                # Format price
                st.metric(
                    label="Precio Estimado",
                    value=f"${predicted_price:,.0f} COP"
                )
                
                # Show details
                with st.expander("Ver detalles técnicos"):
                    st.json(payload)
                    
            else:
                st.error(f"Error del servidor: {response.text}")
                
    except requests.exceptions.ConnectionError:
        st.error("❌ No se pudo conectar con el servicio de predicción. Asegúrate de que el API esté corriendo (docker-compose up o uvicorn).")
    except Exception as e:
        st.error(f"Ocurrió un error inesperado: {e}")

