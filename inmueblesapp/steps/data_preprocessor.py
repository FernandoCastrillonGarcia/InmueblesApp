from zenml import step
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, OneHotEncoder, FunctionTransformer, QuantileTransformer
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
import numpy as np
import pandas as pd
import json
from typing import Tuple, Annotated

from preprocess import DropNullColumns, TrimmColumns, clip_outliers

@step
def build_features_pipelines() -> Tuple[
    Annotated[ColumnTransformer, "X_preprocessor"],
    Annotated[Pipeline, "y_preprocessor"]
]:
    """Preprocess data: split, transform, scale"""

        # Load config
    with open('src/config.json', 'r') as f:
        config_dict = json.load(f)

    numeric_features = config_dict['numeric_features']
    categorical_features = config_dict['categorical_features']

    # DELETE PIPELINE
    cutter = Pipeline([
        ('dropna', DropNullColumns()),
        ('trimmer', TrimmColumns(['AREA','PRICE'], tail = 'upper')),  
    ])

    numeric_transformer = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        # ("log_transform", FunctionTransformer(np.log1p, validate=True)),
        ("scaler", StandardScaler())
    ])

    categorical_transformer = Pipeline([
        ("encoder", OneHotEncoder(handle_unknown="ignore", sparse_output=False))
    ])

    X_preprocessor = ColumnTransformer(
        transformers=[
            ("num", numeric_transformer, numeric_features),
            ("cat", categorical_transformer, categorical_features),
        ]
    )

    y_preprocessor = Pipeline([
        ('winsorize',FunctionTransformer(clip_outliers, inverse_func=None, kw_args={'percentile': 95})),
        ("log_transform", FunctionTransformer(np.log1p, inverse_func = np.expm1)),
        ("quantile", QuantileTransformer(output_distribution='normal')),
        #("scaler", StandardScaler())
    ])

    return X_preprocessor, y_preprocessor

@step
def split_data(df:pd.DataFrame, config_dict:dict)->Tuple[
    Annotated[pd.DataFrame, "X_train"],
    Annotated[pd.DataFrame, "X_test"],
    Annotated[pd.DataFrame, "y_train"],
    Annotated[pd.DataFrame, "y_test"],
]:

    y_column = config_dict['y_column']
    numeric_features = config_dict['numeric_features']
    categorical_features = config_dict['categorical_features']
    all_features = numeric_features + categorical_features

    # Split the data
    X_train, X_test, y_train, y_test = train_test_split(
        df[all_features],
        df[y_column],
        test_size=0.2,
        random_state=42
    )

    return X_train, X_test, y_train, y_test

@step
def process_data(X_train:pd.DataFrame, X_test:pd.DataFrame, y_train:pd.DataFrame, 
                        X_preprocessor:ColumnTransformer, y_preprocessor:Pipeline)->Tuple[
    Annotated[np.ndarray, "X Train processed"],
    Annotated[np.ndarray, "X test processed"],
    Annotated[np.ndarray, "y train processed"],
    Annotated[Pipeline, "y preprocessor fitted"]
]:

    X_train_processed = X_preprocessor.fit_transform(X_train)
    y_train_processed = y_preprocessor.fit_transform(y_train)
    X_test_processed = X_preprocessor.transform(X_test)

    # TODO: Cambiar a Logger
    print(f"✅ Preprocessed: X_train={X_train_processed.shape}, X_test={X_test_processed.shape}")

    return (X_train_processed, X_test_processed, y_train_processed, y_preprocessor)



    

    