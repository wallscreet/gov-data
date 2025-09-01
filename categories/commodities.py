import pandas as pd
from fredapi import Fred
from dotenv import load_dotenv
import os
# import numpy as np
import categories.inflation_and_prices as ip
from functools import reduce


load_dotenv()

FRED_API_KEY = os.getenv("FRED_API_KEY")

fred = Fred(api_key=FRED_API_KEY)


def _fetch_egg_prices(start_date:str=None, end_date:str=None, freq:str=None, add_nominal:bool=False):
    """
    Average Price: Eggs, Grade A, Large (Cost per Dozen) in U.S. City Average (APU0000708111) | path: /egg-prices | freq default: M
    """
    series = fred.get_series('APU0000708111')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'Eggs Per Dozen']

    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]

    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    # df["Year"] = df["Date"].apply(lambda x: int(x[:4]))
    # df["Month"] = df["Date"].apply(lambda x: int(x[5:7]))
    # df["Day"] = df["Date"].apply(lambda x: int(x[8:]))

    return df


def _fetch_milk_prices(start_date:str=None, end_date:str=None, freq:str=None):
    """
    Average Price: Milk, Fresh, Whole, Fortified (Cost per Gallon/3.8 Liters) in U.S. City Average (APU0000709112) | path: /milk-prices | freq default: M
    """
    series = fred.get_series('APU0000709112')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'Milk Per Gallon']

    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]
    

    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    # df["Year"] = df["Date"].apply(lambda x: int(x[:4]))
    # df["Month"] = df["Date"].apply(lambda x: int(x[5:7]))
    # df["Day"] = df["Date"].apply(lambda x: int(x[8:]))

    return df


def _fetch_ground_beef_prices(start_date:str=None, end_date:str=None, freq:str=None):
    """
    Average Price: Ground Beef, 100% Beef (Cost per Pound/453.6 Grams) in U.S. City Average (APU0000703112) | path: /ground-beef-prices | freq default: M
    """
    series = fred.get_series('APU0000703112')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'Ground Beef 1lb']

    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]

    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    # df["Year"] = df["Date"].apply(lambda x: int(x[:4]))
    # df["Month"] = df["Date"].apply(lambda x: int(x[5:7]))
    # df["Day"] = df["Date"].apply(lambda x: int(x[8:]))

    return df


def _fetch_bread_prices(start_date:str=None, end_date:str=None, freq:str=None):
    """
    Average Price: Bread, White, Pan (Cost per Pound/453.6 Grams) in U.S. City Average (APU0000702111) | path: /bread-prices | freq default: M
    """
    series = fred.get_series('APU0000702111')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'Bread 1lb']

    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]

    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    # df["Year"] = df["Date"].apply(lambda x: int(x[:4]))
    # df["Month"] = df["Date"].apply(lambda x: int(x[5:7]))
    # df["Day"] = df["Date"].apply(lambda x: int(x[8:]))

    return df


def _fetch_chicken_prices(start_date:str=None, end_date:str=None, freq:str=None):
    """
    Average Price: Chicken Breast, Boneless (Cost per Pound/453.6 Grams) in U.S. City Average (APU0000FF1101) | path: /chicken-prices | freq default: M
    """
    series = fred.get_series('APU0000FF1101')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'Chicken 1lb']

    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]

    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    # df["Year"] = df["Date"].apply(lambda x: int(x[:4]))
    # df["Month"] = df["Date"].apply(lambda x: int(x[5:7]))
    # df["Day"] = df["Date"].apply(lambda x: int(x[8:]))

    return df


def _fetch_gas_prices(start_date:str=None, end_date:str=None, freq:str=None):
    """
    Average Price: Gasoline, Unleaded Regular (Cost per Gallon/3.785 Liters) in U.S. City Average (APU000074714) | path: /gas-prices | freq default: M
    """
    series = fred.get_series('APU000074714')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'Gas Per Gallon']

    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]

    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    # df["Year"] = df["Date"].apply(lambda x: int(x[:4]))
    # df["Month"] = df["Date"].apply(lambda x: int(x[5:7]))
    # df["Day"] = df["Date"].apply(lambda x: int(x[8:]))

    return df


def _fetch_electric_prices(start_date:str=None, end_date:str=None, freq:str=None):
    """
    Average Price: Electricity per Kilowatt-Hour in U.S. City Average (APU000072610) | path: /electric-kwh-prices | freq default: M
    """
    series = fred.get_series('APU000072610')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'Electric Per kWh']

    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]

    df['Electric Per kWh'] = round(df['Electric Per kWh'], 2)

    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    # df["Year"] = df["Date"].apply(lambda x: int(x[:4]))
    # df["Month"] = df["Date"].apply(lambda x: int(x[5:7]))
    # df["Day"] = df["Date"].apply(lambda x: int(x[8:]))

    return df


def _fetch_coffee_prices(start_date:str=None, end_date:str=None, freq:str=None):
    """
    Average Price: Coffee, 100%, Ground Roast, All Sizes (Cost per Pound/453.6 Grams) in U.S. City Average (APU0000717311) | path: /coffee-prices | freq default: M
    """
    series = fred.get_series('APU0000717311')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'Coffee 1lb']

    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]
    
    df['Coffee 1lb'] = round(df['Coffee 1lb'], 2)

    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    # df["Year"] = df["Date"].apply(lambda x: int(x[:4]))
    # df["Month"] = df["Date"].apply(lambda x: int(x[5:7]))
    # df["Day"] = df["Date"].apply(lambda x: int(x[8:]))

    return df


def _fetch_bacon_sliced_prices(start_date:str=None, end_date:str=None, freq:str=None):
    """
    Average Price: Bacon, Sliced (Cost per Pound/453.6 Grams) in U.S. City Average (APU0000704111) | path: /bacon-prices | freq default: M
    """
    series = fred.get_series('APU0000704111')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'Bacon 1lb']

    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]
    
    df['Bacon 1lb'] = round(df['Bacon 1lb'], 2)

    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    # df["Year"] = df["Date"].apply(lambda x: int(x[:4]))
    # df["Month"] = df["Date"].apply(lambda x: int(x[5:7]))
    # df["Day"] = df["Date"].apply(lambda x: int(x[8:]))

    return df


def _fetch_all_commodity_prices(start_date:str=None, end_date:str=None):
    """
    Aggregated Dataset with all commodities | path: /all-commodity-prices | freq default: M
    """
    cpi_df = ip._fetch_cpi(start_date=start_date, end_date=end_date)
    bacon_df = _fetch_bacon_sliced_prices(start_date=start_date, end_date=end_date)
    eggs_df = _fetch_egg_prices(start_date=start_date, end_date=end_date)
    milk_df = _fetch_milk_prices(start_date=start_date, end_date=end_date)
    bread_df = _fetch_bread_prices(start_date=start_date, end_date=end_date)
    ground_beef_df = _fetch_ground_beef_prices(start_date=start_date, end_date=end_date)
    coffee_df = _fetch_coffee_prices(start_date=start_date, end_date=end_date)
    gas_df = _fetch_gas_prices(start_date=start_date, end_date=end_date)
    electricity_df = _fetch_electric_prices(start_date=start_date, end_date=end_date)
    chicken_df = _fetch_chicken_prices(start_date=start_date, end_date=end_date) #Chicken data starts 2006

    dfs = [cpi_df, bacon_df, eggs_df, milk_df, bread_df, ground_beef_df, coffee_df, gas_df, electricity_df]

    cleaned = []
    for i, df in enumerate(dfs):
        # space to drop cols or other cleaning needed on added series
        ## 
        cleaned.append(df)
    
    merged_df = reduce(lambda left, right: pd.merge(left, right, on='Date', how='inner'), cleaned)

    # Add Real prices for each commodity based on CPI
    latest_cpi = merged_df['CPI'].iloc[-1]
    commodity_cols = [col for col in merged_df.columns if col not in ['Date', 'CPI']]

    for col in commodity_cols:
        merged_df[f"{col} (Real)"] = round((merged_df[col] * (latest_cpi / merged_df['CPI'])), 2)
    
    drop_cols = ['CPI']
    merged_df = merged_df.drop(columns=drop_cols, axis=1)

    meta_cols = ["Date"]
    commodity_cols = [c for c in merged_df.columns if c not in meta_cols and "(Real)" not in c]

    ordered_cols = meta_cols.copy()
    for col in commodity_cols:
        ordered_cols.append(col)
        real_col = f"{col} (Real)"
        if real_col in merged_df.columns:
            ordered_cols.append(real_col)

    ordered_df = merged_df[ordered_cols]

    return ordered_df