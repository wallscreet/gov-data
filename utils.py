import pandas as pd
from functools import reduce


def scale_for_inflation(cpi_df: pd.DataFrame, from_year: int, to_year: int, amount: float):
    from_year_cpi = cpi_df.loc[cpi_df['Year'] == from_year, 'CPI'].values[0]
    to_year_cpi = cpi_df.loc[cpi_df['Year'] == to_year, 'CPI'].values[0]
    adjusted_value = (amount * (to_year_cpi / from_year_cpi))
    
    return round(adjusted_value, 2)


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