import numpy as np
import pandas as pd
from sklearn.metrics import (
    root_mean_squared_error,
    mean_absolute_error, 
    r2_score,
    mean_absolute_percentage_error,
    explained_variance_score
)

def median_absolute_percentage_error(y_true, y_pred):
    return np.median(np.abs((y_true - y_pred) / y_true))


def units_metrics(y_true, y_pred):
    return {
        'MAE': mean_absolute_error(y_true, y_pred),
        'RMSE': root_mean_squared_error(y_true, y_pred)
    }

def percentual_metrics(y_true, y_pred):
    return {
        'MAPE': mean_absolute_percentage_error(y_true, y_pred),
        'MDAPE': median_absolute_percentage_error(y_true, y_pred),
        'R2': r2_score(y_true, y_pred),
        'explained_variance_score': explained_variance_score(y_true, y_pred)
    }

def residual_metrics(y_true, y_pred):

    # residual metrics
    residuals = y_true - y_pred

    return {
        'Mean Residual': np.mean(residuals),
        'Std Residual': np.std(residuals),
        'Max Error': np.max(np.abs(residuals)),
    }
    
def regression_metrics(y_true, y_pred, title = 'REGRESSION', unit = '', print_values=True):
    y_true = np.array(y_true).flatten()
    y_pred = np.array(y_pred).flatten()
    
    percentual = percentual_metrics(y_true, y_pred)
    units = units_metrics(y_true, y_pred)
    residual = residual_metrics(y_true, y_pred)
    
    if print_values:
        print("=" * 50)
        print(title)
        print("=" * 50)

        
        for k, v in percentual.items():
            print(f"{k}: {v * 100:,.1f} % ")
        print('\n')
        for k, v in units.items():
            print(f"{k}: {v:,.2f} {unit}")
        print('\n')
        for k, v in residual.items():
            print(f"{k}: {v:,.4f}")

    return percentual | units | residual
    
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

    return y_pred_original
