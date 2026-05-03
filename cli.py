import typer
import os

app = typer.Typer(
    name="inmuebles-ml",
    help="🏠 InmueblesApp V2 MLOps CLI - Manage Vertex AI Pipelines",
    add_completion=False
)

@app.command()
def trigger_pipeline(
    model: str = typer.Option("xgboost", "--model", "-m", help="Model type to train (xgboost, lightgbm)"),
    project_id: str = typer.Option(None, "--project", "-p", help="GCP Project ID")
):
    """
    Trigger the end-to-end MLOps pipeline (Scrape -> Clean -> Train) on GCP Vertex AI.
    This replaces the old local ZenML implementation.
    """
    from kfp.compiler import Compiler
    from google.cloud import aiplatform
    from pipelines.training import end_to_end_pipeline
    
    project = project_id or os.getenv("PROJECT_ID", "inmuebles-app-437-v2")
    location = os.getenv("LOCATION", "us-central1")
    if not project:
        typer.secho("❌ Error: GCP Project ID is required. Pass --project or set PROJECT_ID env var.", fg=typer.colors.RED)
        raise typer.Exit(1)
        
    typer.secho(f"🚀 Triggering Vertex AI Pipeline for project {project}...", fg=typer.colors.CYAN)
    try:
        # 1. Compile the Python function into a Vertex AI YAML blueprint
        package_path = 'inmueblesapp_ml_pipeline.yaml'
        Compiler().compile(
            pipeline_func=end_to_end_pipeline,
            package_path=package_path
        )
        
        # 2. Submit the YAML blueprint to Google Cloud
        aiplatform.init(project=project, location=location)
        job = aiplatform.PipelineJob(
            display_name="inmueblesapp-ml-pipeline",
            template_path=package_path,
            parameter_values={
                "project_id": project,
                "model_type": model
            },
            enable_caching=True
        )
        job.submit()
        typer.secho(f"✅ Pipeline submitted successfully! Check your GCP Console.", fg=typer.colors.GREEN)
    except Exception as e:
        typer.secho(f"❌ Failed to submit pipeline: {e}", fg=typer.colors.RED)
        raise typer.Exit(1)

@app.command()
def info():
    """
    Show information about the V2 MLOps pipeline.
    """
    typer.secho("\n🏠 InmueblesApp V2 MLOps (GCP Vertex AI)", fg=typer.colors.CYAN, bold=True)
    typer.echo("="*60)
    typer.echo("This CLI triggers remote ML pipelines on Google Cloud Platform.")
    typer.echo("Pipelines include:")
    typer.echo("  1. Data Scraping (KFP Component)")
    typer.echo("  2. Data Cleaning (KFP Component)")
    typer.echo("  3. Model Training & Registration (KFP Component)")
    typer.echo("\nTo view runs, visit the Google Cloud Console -> Vertex AI -> Pipelines")
    typer.echo("="*60 + "\n")

if __name__ == "__main__":
    app()