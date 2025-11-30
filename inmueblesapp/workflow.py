from enum import Enum

import typer

# Define valid model types as Enum for better autocomplete and validation
class ModelType(str, Enum):
    xgboost = "xgboost"
    lightgbm = "lightgbm"
    random_forest = "random_forest"

app = typer.Typer(
    name="inmuebles-ml",
    help="🏠 InmueblesApp ML Training Pipeline - Train price prediction models",
    add_completion=False
)

@app.command()
def scrape():
    """
    Scrape property data from website.
    
    Example:
        python src/workflow.py scrape
    """
    from pipelines.scrapping.pipeline import scraping_pipeline
    
    typer.secho("🔍 Starting scraping pipeline...", fg=typer.colors.CYAN, bold=True)
    
    try:
        # Run pipeline - outputs stored as artifacts
        scraping_pipeline()
        
        typer.secho(f"✅ Scraping completed!", fg=typer.colors.GREEN, bold=True)
        typer.secho("📊 Check data/raw/ for CSV and ZenML dashboard for details", fg=typer.colors.BLUE)
        
    except Exception as e:
        typer.secho(f"❌ Scraping failed: {e}", fg=typer.colors.RED, bold=True, err=True)
        raise typer.Exit(code=1)

@app.command()
def train(
    model: ModelType = typer.Option(
        ModelType.xgboost,
        "--model",
        "-m",
        help="Model type to train",
        case_sensitive=False
    ),
    compare: bool = typer.Option(
        False,
        "--compare",
        "-c",
        help="Train all models for comparison"
    )
):
    """
    Train a price prediction model for real estate properties.
    
    This command trains machine learning models using your property dataset,
    with automatic hyperparameter optimization via Optuna and experiment
    tracking via MLflow.
    
    Examples:
    
        # Train default model (XGBoost)
        python src/workflow.py train
        
        # Train specific model
        python src/workflow.py train --model lightgbm
        python src/workflow.py train -m random_forest
        
        # Train all models for comparison
        python src/workflow.py train --compare
    """
    from pipelines.training.pipeline import price_prediction_pipeline
    
    if compare:
        typer.secho("🔄 Training all models for comparison...", fg=typer.colors.CYAN, bold=True)
        
        for model_type in ModelType:
            typer.echo(f"\n{'='*60}")
            typer.secho(f"  Training {model_type.value.upper()}", fg=typer.colors.YELLOW, bold=True)
            typer.echo(f"{'='*60}")
            
            try:
                price_prediction_pipeline(model_type=model_type.value)
                typer.secho(f"✅ {model_type.value.upper()} completed", fg=typer.colors.GREEN)
            except Exception as e:
                typer.secho(f"❌ {model_type.value.upper()} failed: {e}", fg=typer.colors.RED, err=True)
                
        typer.echo("\n" + "="*60)
        typer.secho("✅ All models trained!", fg=typer.colors.GREEN, bold=True)
        typer.secho("📊 Check MLflow dashboard to compare results", fg=typer.colors.BLUE)
    else:
        typer.secho(f"🚀 Training {model.value.upper()} model...", fg=typer.colors.CYAN, bold=True)
        
        try:
            price_prediction_pipeline(model_type=model.value)
            typer.secho(f"✅ {model.value.upper()} model trained successfully!", fg=typer.colors.GREEN, bold=True)
            typer.secho("📊 View results in ZenML dashboard", fg=typer.colors.BLUE)
        except Exception as e:
            typer.secho(f"❌ Training failed: {e}", fg=typer.colors.RED, bold=True, err=True)
            raise typer.Exit(code=1)

@app.command()
def info():
    """
    Show information about available models and the ML pipeline.
    """
    typer.secho("\n🏠 InmueblesApp ML Pipeline", fg=typer.colors.CYAN, bold=True)
    typer.echo("="*60)
    
    typer.secho("\n📊 Available Models:", fg=typer.colors.YELLOW, bold=True)
    for model in ModelType:
        typer.echo(f"  • {model.value}")
    
    typer.secho("\n🔧 Pipeline Steps:", fg=typer.colors.YELLOW, bold=True)
    typer.echo("  1. Load Data - Load property dataset from CSV")
    typer.echo("  2. Preprocess - Clean, transform, and split data")
    typer.echo("  3. Train - Optimize hyperparameters and train model")
    
    typer.secho("\n📈 Tracking:", fg=typer.colors.YELLOW, bold=True)
    typer.echo("  • ZenML Dashboard: http://127.0.0.1:8237")
    typer.echo("  • MLflow UI: Access via ZenML dashboard")
    
    typer.echo("\n" + "="*60 + "\n")

if __name__ == "__main__":
    app()