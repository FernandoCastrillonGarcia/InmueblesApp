#!/bin/bash

echo "🏗️  Setting up ZenML development environment..."

# Login to zenml
zenml login --local

# Check if development stack exists
if zenml stack describe development &> /dev/null; then
    echo "✅ Stack 'development' already exists"
    zenml stack set development
else
    echo "📦 Creating development stack..."
    
    # Create components (only if they don't exist)
    zenml orchestrator register local_orchestrator --flavor=local 2>/dev/null || echo "  ↳ Orchestrator already exists"
    zenml artifact-store register local_storage --flavor=local 2>/dev/null || echo "  ↳ Artifact store already exists"
    zenml experiment-tracker register mlflow_tracker --flavor=mlflow --tracking_uri="file:./mlruns" 2>/dev/null || echo "  ↳ Experiment tracker already exists"
    zenml model-registry register mlflow_registry --flavor=mlflow --tracking_uri="file:./mlruns" 2>/dev/null || echo "  ↳ Model registry already exists"
    
    # Create development stack
    zenml stack register development \
      -o local_orchestrator \
      -a local_storage \
      -e mlflow_tracker \
      -r mlflow_registry
    
    # Activate stack
    zenml stack set development
    
    echo "✅ Development stack created and activated"
fi

# Show status
echo ""
echo "📊 Current stack configuration:"
zenml stack describe