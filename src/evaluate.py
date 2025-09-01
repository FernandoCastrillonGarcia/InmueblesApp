import numpy as np
import pandas as pd
from sklearn.metrics import (
    mean_absolute_error, 
    r2_score,
    mean_absolute_percentage_error,
    explained_variance_score
)

def regression_metrics(y_true, y_pred):
    """
    Calculate comprehensive regression metrics.
    
    Parameters:
    -----------
    y_true : array-like
        True target values
    y_pred : array-like
        Predicted values
        
    Returns:
    --------
    dict : Dictionary containing all regression metrics
    """
    
    # Ensure arrays are flattened
    y_true = np.array(y_true).flatten()
    y_pred = np.array(y_pred).flatten()
    
    # Basic metrics
    mae = mean_absolute_error(y_true, y_pred)
    r2 = r2_score(y_true, y_pred)
    
    # Additional metrics
    mape = mean_absolute_percentage_error(y_true, y_pred)
    explained_var = explained_variance_score(y_true, y_pred)
        
    
    return {
        # Primary metrics
        'MAE': mae,
        'R²': r2,
        'MAPE': mape,
        'Explained Variance': explained_var,
    }

def residual_metrics(y_true, y_pred):

    # Custom metrics
    residuals = y_true - y_pred
    mean_residual = np.mean(residuals)
    std_residual = np.std(residuals)

        # Additional useful metrics
    max_error = np.max(np.abs(residuals))
    median_ae = np.median(np.abs(residuals))

    return {
        'Mean Residual': mean_residual,
        'Std Residual': std_residual,
        'Max Error': max_error,
        'Median AE': median_ae
    }
    
def print_metrics(metrics_dict, title = 'REGRESSION'):
    """Pretty print metrics with formatting."""
    print("=" * 50)
    print(f"{title} METRICS")
    print("=" * 50)
    

    for k, v in metrics_dict.items():
        print(f"{k}: {v:.4f}")
        
    
# Usage example
def train_eval_model(models, scores, X_train, y_train, X_test, y_test, y_preprocessor=None):
    """
    Evaluate a trained model and return metrics.
    
    Parameters:
    -----------
    model : trained model
        The trained regression model
    X_test : array-like
        Test features
    y_test : array-like
        True test values
    y_preprocessor : sklearn transformer, optional
        Preprocessor to inverse transform predictions
        
    Returns:
    --------
    dict : Dictionary containing all metrics
    """
    
    best_model = models[scores.index(max(scores))]
    best_model = models[-1]
    best_model.fit(X_train, y_train)
    
    # Get predictions
    y_pred = best_model.predict(X_test)
    
    if isinstance(y_test, pd.DataFrame):
        y_test = y_test.values

    # Inverse transform if preprocessor provided
    if y_preprocessor is not None:
        y_pred_original = y_preprocessor.inverse_transform(y_pred.reshape(-1, 1)).flatten()
    else:
        y_pred_original = y_pred
    
    
    # Calculate metrics
    metrics = regression_metrics(y_test, y_pred_original)
    
    print("="*20,"Best model","="*20)
    print(best_model)
    print("\n")
    # Print results
    print_metrics(metrics)
