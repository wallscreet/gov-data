import pandas as pd
from functools import reduce
import numpy as np


def scale_for_inflation(cpi_df: pd.DataFrame, from_year: int, to_year: int, amount: float):
    from_year_cpi = cpi_df.loc[cpi_df['Year'] == from_year, 'CPI'].values[0]
    to_year_cpi = cpi_df.loc[cpi_df['Year'] == to_year, 'CPI'].values[0]
    adjusted_value = (amount * (to_year_cpi / from_year_cpi))
    
    return round(adjusted_value, 2)


def merge_on_date(dfs, how='inner'):
    cleaned = []
    for i, df in enumerate(dfs):
        if 'Date' not in df.columns:
            raise ValueError(f"DataFrame at index {i} is missing 'Date' column.")

        # Drop Year/Month/Day if present
        drop_cols = [c for c in ['Year', 'Month', 'Day'] if c in df.columns]
        df = df.drop(columns=drop_cols)

        cleaned.append(df)

    merged_df = reduce(lambda left, right: pd.merge(left, right, on='Date', how=how), cleaned)
    return merged_df


def merge_on_year(dfs, how='inner'):
    """
    Merge a list of dataframes on the 'Year' column.
    """
    # Safety check: make sure they all have 'Year' column
    for i, df in enumerate(dfs):
        if 'Year' not in df.columns:
            raise ValueError(f"DataFrame at index {i} is missing 'Year' column.")

    merged_df = reduce(lambda left, right: pd.merge(left, right, on='Year', how=how), dfs)
    
    return merged_df


def calc_mtg_pi_payment(principal, annual_rate, years=30):
    """
    Calculate monthly principal & interest payment for a mortgage.
    """
    monthly_rate = (annual_rate / 100) / 12
    n_payments = years * 12
    
    if monthly_rate == 0:
        return principal / n_payments
    
    payment = principal * (monthly_rate * (1 + monthly_rate) ** n_payments) / \
              ((1 + monthly_rate) ** n_payments - 1)
    
    return payment


def sanitize_for_json(df: pd.DataFrame) -> list[dict]:
    """Convert a DataFrame into JSON-safe records."""
    safe_df = df.replace({np.nan: None})
    return safe_df.to_dict(orient="records")


def add_real_prices(df):
    latest_cpi = df['CPI'].iloc[-1]
    commodity_cols = [col for col in df.columns if col not in ["Date", "CPI"]]

    # scale each nominal price into real 2025 dollars
    for col in commodity_cols:
        df[f"{col} (Real)"] = round((df[col] * (latest_cpi / df["CPI"])),2)
    
    return df