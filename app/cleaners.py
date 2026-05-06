import re
import pandas as pd


def snake_case_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename all columns to lowercase snake_case."""
    df = df.copy()
    df.columns = [
        re.sub(r'[^a-z0-9]+', '_', col.strip().lower()).strip('_')
        for col in df.columns
    ]
    return df


def parse_date_column(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """Parse a single column to datetime, coercing unparseable values to NaT."""
    if col in df.columns:
        df = df.copy()
        df[col] = pd.to_datetime(df[col], errors='coerce')
    return df


def drop_empty_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows where every column is null."""
    return df.dropna(how='all').reset_index(drop=True)
