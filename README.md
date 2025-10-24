# InmueblesApp 🏠

A real estate recommendation application built with Streamlit, powered by Ollama embeddings and Qdrant vector search.

## Screenshots

### Main Page
![Main Page](docs/images/main_page.png)

### Recommendation Page
![Recommendation Page](docs/images/recommendation_page.png)

---

## 🚀 Local Deployment with Docker

### Prerequisites
- Docker and Docker Compose installed
- Git

### Setup Instructions

1. **Clone the repository**
```bash
git clone <repository-url>
cd InmueblesApp
```

2. **Create environment file**
```bash
# Create .env file with your configuration
cat > .env << EOF
OLLAMA_HOST=http://ollama:11434
QDRANT_HOST=http://qdrant:6333
QDRANT_API_KEY=your_qdrant_api_key_here
EOF
```

3. **Build and run with Docker Compose**
```bash
# Build the images
docker compose build

# Start all services (Streamlit, Ollama, Qdrant)
docker compose up
```

4. **Access the application**
- Open your browser at: `http://localhost:8501`
- The app will automatically pull the `nomic-embed-text` model on first startup

5. **Stop the services**
```bash
docker compose down
```

### Local Architecture
The `docker-compose.yml` orchestrates three services:
- **Streamlit**: Web application (port 8501)
- **Ollama**: Embedding generation service (internal)
- **Qdrant**: Vector database (port 6333)

---

## ☁️ Cloud Deployment (Google Cloud Platform)

### Prerequisites
- Google Cloud account with billing enabled
- `gcloud` CLI installed and authenticated

### Setup Instructions

1. **Configure your project**

Edit `cloud-setting.sh` and replace placeholders:
```bash
PROJECT_ID=your-unique-project-id

```

2. **Run the setup script**
```bash
chmod +x cloud-setting.sh
./cloud-setting.sh
```

This script will:
- Create a new GCP project
- Enable required APIs (Cloud Run, Cloud Build, Artifact Registry, Secret Manager)
- Configure IAM permissions for service accounts
- Store Qdrant credentials in Secret Manager
- Create Docker image repository
- Deploy both services to Cloud Run

3. **Get your deployed URLs**
```bash
# Streamlit App URL
gcloud run services describe streamlit-app --region=us-east1 --format="value(status.url)"

# Ollama Service URL
gcloud run services describe ollama-service --region=us-east1 --format="value(status.url)"
```

4. **Update deployment**
```bash
# After making code changes, redeploy with:
gcloud builds submit --config cloudbuild.yaml
```

### Cloud Architecture
The `cloudbuild.yaml` deploys two Cloud Run services:
- **Ollama Service**: Pre-loaded with `nomic-embed-text` model (4 CPU, 8GB RAM)
- **Streamlit App**: Web application that connects to Ollama and Qdrant Cloud (1 CPU, 2GB RAM)

### Cost Optimization
- Services scale to zero when not in use (pay-per-request)
- Ollama service limited to 1 instance max
- Streamlit app auto-scales based on traffic

---

## 📁 Project Structure

```
InmueblesApp/
├── src/
│   ├── app.py              # Main Streamlit application
│   ├── embedding.py        # Ollama embedding functions
│   ├── database.py         # Qdrant database operations
│   └── pages/
│       └── recommendation.py
├── Dockerfile              # Local development
├── Dockerfile.streamlit    # Cloud Run - Streamlit
├── Dockerfile.ollama       # Cloud Run - Ollama
├── docker-compose.yml      # Local multi-service orchestration
├── cloudbuild.yaml         # GCP deployment configuration
├── cloud-setting.sh        # GCP setup script
├── pyproject.toml          # Python dependencies (uv)
└── uv.lock                 # Locked dependencies
```

---

## 🛠️ Technology Stack

- **Frontend**: Streamlit
- **Embeddings**: Ollama (nomic-embed-text model)
- **Vector Database**: Qdrant
- **Dependency Management**: uv
- **Containerization**: Docker
- **Cloud Platform**: Google Cloud Run

---

## 📝 Environment Variables

### Local Development (.env)
```bash
OLLAMA_HOST=http://ollama:11434
QDRANT_HOST=http://qdrant:6333
QDRANT_API_KEY=your_key
```

### Cloud Deployment (Secret Manager)
- `QDRANT_HOST`: Qdrant Cloud cluster URL
- `QDRANT_API_KEY`: Qdrant Cloud API key
- `OLLAMA_HOST`: Dynamically set to deployed Ollama service URL

---

## 🔐 Security Notes

- Secrets are stored in Google Secret Manager (cloud deployment)
- Service accounts follow principle of least privilege
- Cloud Run services can be configured for authenticated access only

---

## 📄 License

[Your License Here]

---

## 👤 Author

[Your Name]
