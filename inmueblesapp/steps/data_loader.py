from zenml import step
import pandas as pd

@step
def load_data() -> pd.DataFrame:
    """Load raw property data"""
    df = pd.read_csv('data/raw/data_v1.csv', sep=';')
    print(f"Loaded {len(df)} properties")
    return df