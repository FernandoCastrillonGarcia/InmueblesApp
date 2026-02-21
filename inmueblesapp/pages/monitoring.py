import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from wordcloud import WordCloud
from database import MongoSingleton

# ──────────────────────────── Page config ────────────────────────────
st.set_page_config(page_title="Monitor de Datos", page_icon="📈", layout="wide")
st.title("📈 Monitor de Scraping y Mercado")


LOCAL = os.getenv("LOCAL", "true").lower() == "true"
OPERATIONS = ["Arriendo", "Venta"]

# ──────────────────────────── DB connection ──────────────────────────
try:
    client = MongoSingleton(local=LOCAL).get_client()
    db = client["inmuebles_db"]
except Exception as e:
    st.error(f"Error conectando a MongoDB: {e}")
    st.stop()

# ──────────────────────────── Load data ──────────────────────────────
PROJECTION = {
    "_id": 0,
    "PRICE": 1,
    "AREA": 1,
    "BUILT_AREA": 1,
    "PROPERTY_TYPE": 1,
    "OPERATION_TYPE": 1,
    "STRATUM": 1,
    "ANTIQUITY": 1,
    "ROOMS": 1,
    "BEDROOMS": 1,
    "BATHROOMS": 1,
    "GARAGE": 1,
    "DESCRIPTION": 1,
    "scraped_at": 1,
    "batch_id": 1,
}

NUMERIC_COLS = ["PRICE", "AREA", "BUILT_AREA", "ROOMS", "BEDROOMS", "BATHROOMS", "GARAGE", "STRATUM"]


@st.cache_data(ttl=600)
def load_data() -> pd.DataFrame:
    """Load data from all operation collections and concatenate."""
    frames: list[pd.DataFrame] = []
    for op in OPERATIONS:
        if op not in db.list_collection_names():
            continue
        docs = list(db[op].find({}, PROJECTION))
        if docs:
            tmp = pd.DataFrame(docs)
            tmp["OPERATION_TYPE"] = tmp.get("OPERATION_TYPE", op)
            frames.append(tmp)

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "scraped_at" in df.columns:
        df["scraped_at"] = pd.to_datetime(df["scraped_at"], errors="coerce")
    return df


with st.spinner("Cargando datos…"):
    df = load_data()

if df.empty:
    st.warning("No hay datos en la base de datos.")
    st.stop()

# ──────────────────────────── Sidebar filters ────────────────────────
with st.sidebar:
    st.header("Filtros")
    sel_operation = st.multiselect(
        "Operación", df["OPERATION_TYPE"].dropna().unique().tolist(),
        default=df["OPERATION_TYPE"].dropna().unique().tolist(),
    )
    sel_property = st.multiselect(
        "Tipo de propiedad", df["PROPERTY_TYPE"].dropna().unique().tolist(),
        default=df["PROPERTY_TYPE"].dropna().unique().tolist(),
    )

mask = df["OPERATION_TYPE"].isin(sel_operation) & df["PROPERTY_TYPE"].isin(sel_property)
fdf = df.loc[mask].copy()

if fdf.empty:
    st.info("No hay datos para los filtros seleccionados.")
    st.stop()

# ──────────────────────────── KPI row ────────────────────────────────
st.markdown("### 🛡️ Resumen General")
k1, k2, k3, k4 = st.columns(4)
k1.metric("Total registros", f"{len(fdf):,}")
k2.metric("Precio medio", f"${fdf['PRICE'].mean() / 1e6:,.1f} M")
k3.metric("Área media", f"{fdf['AREA'].mean():,.0f} m²")
latest_batch = fdf["batch_id"].max() if "batch_id" in fdf.columns else "N/A"
k4.metric("Último batch", str(latest_batch)[:16])

st.divider()

# ═══════════════════ 1. DISTRIBUCIONES CONTINUAS ═════════════════════
st.markdown("## 📊 Distribuciones Continuas")

fdf["PRICE_M"] = fdf["PRICE"] / 1e6

col1, col2 = st.columns(2)

with col1:
    fig_price = px.histogram(
        fdf.dropna(subset=["PRICE_M"]),
        x="PRICE_M",
        color="OPERATION_TYPE",
        nbins=60,
        marginal="box",
        title="Distribución de Precio (Millones COP)",
        labels={"PRICE_M": "Precio [M COP]", "OPERATION_TYPE": "Operación"},
        opacity=0.7,
        barmode="overlay",
    )
    fig_price.update_layout(legend=dict(orientation="h", y=-0.25))
    st.plotly_chart(fig_price, use_container_width=True)

with col2:
    area_col = "BUILT_AREA" if fdf["BUILT_AREA"].notna().sum() > 100 else "AREA"
    area_label = "Área Construida" if area_col == "BUILT_AREA" else "Área"
    fig_area = px.histogram(
        fdf.dropna(subset=[area_col]),
        x=area_col,
        color="OPERATION_TYPE",
        nbins=60,
        marginal="box",
        title=f"Distribución de {area_label} (m²)",
        labels={area_col: f"{area_label} [m²]", "OPERATION_TYPE": "Operación"},
        opacity=0.7,
        barmode="overlay",
    )
    fig_area.update_layout(legend=dict(orientation="h", y=-0.25))
    st.plotly_chart(fig_area, use_container_width=True)

# Violin: price by property type (top 6 types)
top_types = fdf["PROPERTY_TYPE"].value_counts().head(6).index.tolist()
violin_df = fdf.loc[fdf["PROPERTY_TYPE"].isin(top_types)].dropna(subset=["PRICE_M"])

if not violin_df.empty:
    fig_violin = px.violin(
        violin_df,
        x="PROPERTY_TYPE",
        y="PRICE_M",
        color="OPERATION_TYPE",
        box=True,
        title="Precio por Tipo de Propiedad (Top 6)",
        labels={"PRICE_M": "Precio [M COP]", "PROPERTY_TYPE": "Tipo", "OPERATION_TYPE": "Operación"},
    )
    fig_violin.update_layout(legend=dict(orientation="h", y=-0.2))
    st.plotly_chart(fig_violin, use_container_width=True)

st.divider()

# ═══════════════════ 2. VARIABLES CATEGÓRICAS ════════════════════════
st.markdown("## 📋 Variables Categóricas")

cat_col1, cat_col2 = st.columns(2)

with cat_col1:
    prop_counts = fdf["PROPERTY_TYPE"].value_counts().reset_index()
    prop_counts.columns = ["Tipo", "Cantidad"]
    fig_prop = px.bar(
        prop_counts,
        x="Cantidad",
        y="Tipo",
        orientation="h",
        title="Propiedades por Tipo",
        color="Cantidad",
        color_continuous_scale="Tealgrn",
    )
    fig_prop.update_layout(yaxis=dict(categoryorder="total ascending"), coloraxis_showscale=False)
    st.plotly_chart(fig_prop, use_container_width=True)

with cat_col2:
    if "STRATUM" in fdf.columns and fdf["STRATUM"].notna().sum() > 0:
        strat_counts = (
            fdf.dropna(subset=["STRATUM"])
            .assign(STRATUM=lambda d: d["STRATUM"].astype(int).astype(str))
            ["STRATUM"]
            .value_counts()
            .sort_index()
            .reset_index()
        )
        strat_counts.columns = ["Estrato", "Cantidad"]
        fig_strat = px.bar(
            strat_counts,
            x="Estrato",
            y="Cantidad",
            title="Distribución por Estrato",
            color="Cantidad",
            color_continuous_scale="Sunset",
        )
        fig_strat.update_layout(coloraxis_showscale=False)
        st.plotly_chart(fig_strat, use_container_width=True)

cat_col3, cat_col4 = st.columns(2)

with cat_col3:
    if "ANTIQUITY" in fdf.columns and fdf["ANTIQUITY"].notna().sum() > 0:
        antiq_counts = fdf["ANTIQUITY"].value_counts().reset_index()
        antiq_counts.columns = ["Antigüedad", "Cantidad"]
        fig_antiq = px.pie(
            antiq_counts,
            names="Antigüedad",
            values="Cantidad",
            title="Antigüedad de las Propiedades",
            hole=0.4,
        )
        st.plotly_chart(fig_antiq, use_container_width=True)

with cat_col4:
    # Rooms / Bedrooms / Bathrooms counts
    count_vars = ["BEDROOMS", "BATHROOMS", "GARAGE"]
    available_counts = [c for c in count_vars if c in fdf.columns and fdf[c].notna().sum() > 0]
    if available_counts:
        selected_count = st.selectbox("Variable de conteo", available_counts)
        cnt = (
            fdf.dropna(subset=[selected_count])
            .assign(**{selected_count: lambda d: d[selected_count].astype(int).astype(str)})
            [selected_count]
            .value_counts()
            .sort_index()
            .reset_index()
        )
        cnt.columns = [selected_count, "Cantidad"]
        fig_cnt = px.bar(
            cnt,
            x=selected_count,
            y="Cantidad",
            title=f"Distribución de {selected_count}",
            color="Cantidad",
            color_continuous_scale="Blugrn",
        )
        fig_cnt.update_layout(coloraxis_showscale=False)
        st.plotly_chart(fig_cnt, use_container_width=True)

st.divider()

# ═══════════════════ 3. WORD CLOUD ═══════════════════════════════════
st.markdown("## ☁️ Nube de Palabras — Descripciones")

if "DESCRIPTION" in fdf.columns and fdf["DESCRIPTION"].notna().sum() > 0:
    # Sample to avoid excessive memory usage on large datasets
    sample_size = min(5_000, fdf["DESCRIPTION"].notna().sum())
    text = " ".join(fdf["DESCRIPTION"].dropna().sample(sample_size, random_state=42).tolist())

    STOPWORDS_ES = {
        "de", "la", "el", "en", "y", "a", "los", "del", "las", "un", "una",
        "con", "para", "por", "que", "se", "es", "al", "lo", "como", "más",
        "su", "sus", "está", "este", "esta", "son", "o", "no", "hay", "nos",
        "muy", "sin", "sobre", "todo", "ya", "pero", "le", "ha", "me", "mi",
        "si", "te", "ti", "tu", "fue", "ser", "tiene", "también", "entre",
        "cuando", "ese", "eso", "esa", "cada", "uno", "dos", "tres",
    }

    wc = WordCloud(
        width=1200,
        height=500,
        background_color="white",
        colormap="viridis",
        max_words=150,
        stopwords=STOPWORDS_ES,
        collocations=False,
    ).generate(text)

    st.image(wc.to_array(), use_container_width=True)
else:
    st.info("No hay descripciones disponibles para generar la nube de palabras.")
