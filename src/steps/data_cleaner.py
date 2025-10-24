from zenml import step
import pandas as pd
import json
from typing import Annotated, Tuple

@step
def drop_columns(df_raw:pd.DataFrame, config_dict:dict
) -> Annotated[pd.DataFrame, "Target and Features"]:
    """
    Loads the config of the features and drop unnecesary columns. Only keeps features and target.
    """ 

    y_column = config_dict['y_column']
    numeric_features = config_dict['numeric_features']
    categorical_features = config_dict['categorical_features']
    all_features = numeric_features + categorical_features
    
    # Use y and X columns only (Delete Indexes and Ids)
    df = df_raw.loc[:,y_column + all_features]

    return df

@step
def fill_categorical_features(df:pd.DataFrame, config_dict:dict) -> Annotated[pd.DataFrame, "Filled Categorical Values"]:
    """
    Fill Null values from categorical data wih 'No se sabe' if it is string and 999 if it is numerical
    """
    categorical_features = config_dict['categorical_features']

    # Categorical features can"t have NaN
    for c in categorical_features:

        # Default Value for Nan numeric
        if pd.api.types.is_numeric_dtype(df[c]):
            df[c] = df[c].fillna(999)

        # Devault Value for Nan String
        elif pd.api.types.is_object_dtype(df[c]):
            df[c] = df[c].fillna('No se sabe')

    df[categorical_features] = df[categorical_features].astype('category')

    return df

@step
def manual_filtering(df:pd.DataFrame) -> Annotated[pd.DataFrame, "Dropped useless rows"]:

    old_len = len(df)

    # Drop erroneous data
    df = df.loc[df['FLOOR'] != 202]
    df = df.loc[df['STRATUM'] != 101]

    # Apply area and price filters only for Apartamento and Apartaestudio
    apartment_mask = df['PROPERTY_TYPE'].isin(['Apartamento', 'Apartaestudio'])
    df.loc[apartment_mask] = df.loc[apartment_mask
                                    & (df['AREA'] < 400)
                                    & (df['AREA'] > 0)
                                    & (df['PRICE'] < 20_000_000)
                                    & (df['PRICE'] > 100_000)]

    # Drop remaining NaN
    df.dropna(inplace=True)

    new_len=len(df)

    # TODO: Cambiar a Logger
    print(f"Se removieron {old_len - new_len:,} observacion. un total del {(old_len - new_len) * 100/old_len:,.0f}% de la muestra original")

    return df

