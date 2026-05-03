from pipelines.training import train_model_op

if __name__ == "__main__":
    print("🧪 Testing Training Pipeline Component...")
    
    # We test with XGBoost by default, but you can switch to 'lightgbm' or 'random_forest'
    # Running this will execute the entire MLflow training sequence locally
    try:
        result_metrics_json = train_model_op.python_func(model_type="xgboost")
        print(f"\n✅ Training completed locally!")
        print(f"Metrics (JSON): {result_metrics_json}")
    except Exception as e:
        print(f"\n❌ Error during training: {e}")
