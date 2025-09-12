from sklearn.preprocessing import StandardScaler, OneHotEncoder, FunctionTransformer
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline


import numpy as np

import json


from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.preprocessing import StandardScaler, OneHotEncoder
import pandas as pd
import numpy as np


class DropNullColumns(BaseEstimator, TransformerMixin):
    """
    Custom transformer template for data processing pipelines.
    
    This class follows scikit-learn's transformer interface and can be used
    in pipelines with fit(), transform(), and fit_transform() methods.
    """
    
    def __init__(self, threshold=0.5):
        """
        Initialize the transformer with parameters.
        
        Parameters:
        -----------
        feature_name : str, optional
            Name of the feature to transform
        operation : str, default='default'
            Type of operation to perform
        threshold : float, optional
            Threshold value for operations
        """
        self.threshold = threshold
        
        # These will be set during fit()
        self.fitted = None
    
    def fit(self, X, y=None):
        """
        Learn parameters from the training data.
        
        Parameters:
        -----------
        X : array-like of shape (n_samples, n_features)
            Training data
        y : array-like of shape (n_samples,), optional
            Target values (ignored in unsupervised transformers)
            
        Returns:
        --------
        self : object
            Returns the instance itself
        """
        # Expect DataFrame
        if not isinstance(X, pd.DataFrame):
            raise TypeError("Esperaba un DataFrame")
        
        # Store feature names
        self.feature_names_in = X.columns.tolist()
        self.features_names_out = []
        for col in self.feature_names_in:
            null_values = X[col].isna().sum()

            if (null_values) / len(X[col]) > self.threshold:
                self.features_names_out.append(col)
        
        self.fitted = True
        return self
                
    
    def transform(self, X)->pd.DataFrame:
        """
        Transform the input data.
        
        Parameters:
        -----------
        X : array-like of shape (n_samples, n_features)
            Data to transform
            
        Returns:
        --------
        X_transformed : array-like of shape (n_samples, n_features_out)
            Transformed data
        """
        # Check if fit has been called
        if not self.fitted:
            raise ValueError("This transformer has not been fitted yet. Call 'fit' first.")
        
        # Expect DataFrame
        if not isinstance(X, pd.DataFrame):
            raise TypeError("Esperaba un DataFrame")
        
        # Create a copy to avoid modifying original data
        X_transformed = X.copy()
        
        if self.features_names_out:
            X_transformed = X_transformed.drop(columns = self.features_names_out)
            print(f"These columns were droped, you can retrive them in features_names_out attr: {self.features_names_out}")
        else:
            print("No columns were Droped")
        
        return X_transformed
    

class TrimmColumns(BaseEstimator, TransformerMixin):
    """
    
    """
    def __init__(self, col_names, quantile = 0.5, tail='both'):

        self.quantile = quantile
        self.tail=tail
        self.col_names = col_names
        
    
    def fit(self, X, y=None):
        
        if isinstance(self.quantile, (float, int)):
           self.quantile = [self.quantile] * len(self.col_names)

        if isinstance(self.tail, str):
           self.tail = [self.tail] * len(self.col_names)

        
        self.columns_quantiles = []
        for i, col  in enumerate(self.col_names):
            
            if col in X.columns:
                if self.tail[i] == ('upper'):
                    self.columns_quantiles.append(np.quantile(X[col], q = 1 - self.quantile[i]))

                elif self.tail[i] == ('lower'):
                    self.columns_quantiles.append(np.quantile(X[col], q = self.quantile[i]))

                elif self.tail[i] == ('both'):
                    self.columns_quantiles.append(
                        (np.quantile(X[col], q = self.quantile[i]), np.quantile(X[col], q = 1 - self.quantile[i]))
                    )         

        return self

    def transform(self, X, y=None):
        
        X_transformed = X.copy()
        
        for c, q, tail in zip(self.col_names, self.columns_quantiles, self.tail):
            
            if c in X.columns:
                if tail == 'both':
                    X_transformed = X_transformed[(q[0] <= X_transformed[c]) & (X_transformed[c] <= q[1])]
                
                elif tail == 'upper':
                    X_transformed = X_transformed[X_transformed[c] <= q]
                
                elif tail == 'lower':
                    X_transformed = X_transformed[q <= X_transformed[c]]
                
                else:
                    raise ValueError("Bruh")
        
        return X_transformed

def clip_outliers(y, percentile=95):
    """Clip values above the specified percentile."""
    upper_limit = np.percentile(y, percentile)
    return np.clip(y, a_min=None, a_max=upper_limit)


