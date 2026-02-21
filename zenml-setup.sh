#!/bin/bash
# ============================================================
#  ZenML local development stack setup
#  Run this on your LOCAL machine before training pipelines.
#  This has nothing to do with Cloud Run — it orchestrates
#  scraping, cleaning, and training locally.
# ============================================================
set -euo pipefail

echo "🏗️  Setting up ZenML development environment..."

# Login to local ZenML server
zenml login --local

# Install required integrations
echo "🔌 Installing MLflow integration..."
zenml integration install mlflow -y --uv

# Check if development stack already exists
if zenml stack describe development &> /dev/null; then
    echo "✅ Stack 'development' already exists"
    zenml stack set development
else
    echo "📦 Creating development stack..."

    # Register components (idempotent — errors are silenced)
    zenml orchestrator register local_orchestrator --flavor=local 2>/dev/null \
      || echo "  ↳ Orchestrator already exists"
    zenml artifact-store register local_storage --flavor=local --path="$(pwd)/mlruns" 2>/dev/null \
      || echo "  ↳ Artifact store already exists"
    zenml experiment-tracker register mlflow_tracker --flavor=mlflow 2>/dev/null \
      || echo "  ↳ Experiment tracker already exists"
    zenml model-registry register mlflow_registry --flavor=mlflow 2>/dev/null \
      || echo "  ↳ Model registry already exists"

    # Assemble and activate stack
    zenml stack register development \
      -o local_orchestrator \
      -a local_storage \
      -e mlflow_tracker \
      -r mlflow_registry

    zenml stack set development
    echo "✅ Development stack created and activated"
fi

# Show current config
echo ""
echo "📊 Current stack configuration:"
zenml stack describe
