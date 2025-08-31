from http.client import HTTPException
import pandas as pd
from fredapi import Fred
from dotenv import load_dotenv
import os
import json
from utils import merge_on_year, scale_for_inflation, calc_mtg_pi_payment
import numpy as np

load_dotenv()

FRED_API_KEY = os.getenv("FRED_API_KEY")

fred = Fred(api_key=FRED_API_KEY)


def _fetch_egg_prices(start_date:str=None, end_date:str=None, freq:str=None):
    """
    Average Price: Eggs, Grade A, Large (Cost per Dozen) in U.S. City Average (APU0000708111) | path: /egg-prices | freq default: M | freqs available: Q
    """
    series = fred.get_series('APU0000708111')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'Eggs Per Dozen']

    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]
    
    if freq.upper() == 'Q':
        df = df.set_index("Date").resample('QE').mean().reset_index()

    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    df["Year"] = df["Date"].apply(lambda x: int(x[:4]))
    df["Month"] = df["Date"].apply(lambda x: int(x[5:7]))
    df["Day"] = df["Date"].apply(lambda x: int(x[8:]))

    return df


def _fetch_milk_prices(start_date:str=None, end_date:str=None, freq:str=None):
    """
    Average Price: Milk, Fresh, Whole, Fortified (Cost per Gallon/3.8 Liters) in U.S. City Average (APU0000709112) | path: /milk-prices | freq default: M | freqs available: Q
    """
    series = fred.get_series('APU0000709112')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'Milk Per Gallon']

    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]
    
    if freq.upper() == 'Q':
        df = df.set_index("Date").resample('QE').mean().reset_index()

    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    df["Year"] = df["Date"].apply(lambda x: int(x[:4]))
    df["Month"] = df["Date"].apply(lambda x: int(x[5:7]))
    df["Day"] = df["Date"].apply(lambda x: int(x[8:]))

    return df


def _fetch_ground_beef_prices(start_date:str=None, end_date:str=None, freq:str=None):
    """
    Average Price: Ground Beef, 100% Beef (Cost per Pound/453.6 Grams) in U.S. City Average (APU0000703112) | path: /ground-beef-prices | freq default: M | freqs available: Q
    """
    series = fred.get_series('APU0000703112')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'Ground Beef 1lb']

    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]
    
    if freq.upper() == 'Q':
        df = df.set_index("Date").resample('QE').mean().reset_index()

    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    df["Year"] = df["Date"].apply(lambda x: int(x[:4]))
    df["Month"] = df["Date"].apply(lambda x: int(x[5:7]))
    df["Day"] = df["Date"].apply(lambda x: int(x[8:]))

    return df


def _fetch_bread_prices(start_date:str=None, end_date:str=None, freq:str=None):
    """
    Average Price: Bread, White, Pan (Cost per Pound/453.6 Grams) in U.S. City Average (APU0000702111) | path: /bread-prices | freq default: M | freqs available: Q
    """
    series = fred.get_series('APU0000702111')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'Bread 1lb']

    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]
    
    if freq.upper() == 'Q':
        df = df.set_index("Date").resample('QE').mean().reset_index()

    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    df["Year"] = df["Date"].apply(lambda x: int(x[:4]))
    df["Month"] = df["Date"].apply(lambda x: int(x[5:7]))
    df["Day"] = df["Date"].apply(lambda x: int(x[8:]))

    return df


def _fetch_chicken_prices(start_date:str=None, end_date:str=None, freq:str=None):
    """
    Average Price: Chicken Breast, Boneless (Cost per Pound/453.6 Grams) in U.S. City Average (APU0000FF1101) | path: /chicken-prices | freq default: M | freqs available: Q
    """
    series = fred.get_series('APU0000FF1101')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'Chicken 1lb']

    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]
    
    if freq.upper() == 'Q':
        df = df.set_index("Date").resample('QE').mean().reset_index()

    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    df["Year"] = df["Date"].apply(lambda x: int(x[:4]))
    df["Month"] = df["Date"].apply(lambda x: int(x[5:7]))
    df["Day"] = df["Date"].apply(lambda x: int(x[8:]))

    return df


def _fetch_gas_prices(start_date:str=None, end_date:str=None, freq:str=None):
    """
    Average Price: Gasoline, Unleaded Regular (Cost per Gallon/3.785 Liters) in U.S. City Average (APU000074714) | path: /gas-prices | freq default: M | freqs available: Q
    """
    series = fred.get_series('APU000074714')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'Gas Per Gallon']

    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]
    
    if freq.upper() == 'Q':
        df = df.set_index("Date").resample('QE').mean().reset_index()

    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    df["Year"] = df["Date"].apply(lambda x: int(x[:4]))
    df["Month"] = df["Date"].apply(lambda x: int(x[5:7]))
    df["Day"] = df["Date"].apply(lambda x: int(x[8:]))

    return df


def _fetch_electric_prices(start_date:str=None, end_date:str=None, freq:str=None):
    """
    Average Price: Electricity per Kilowatt-Hour in U.S. City Average (APU000072610) | path: /electric-kwh-prices | freq default: M | freqs available: Q
    """
    series = fred.get_series('APU000072610')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'Electric Per kWh']

    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]
    
    if freq.upper() == 'Q':
        df = df.set_index("Date").resample('QE').mean().reset_index()

    df['Electric Per kWh'] = round(df['Electric Per kWh'], 2)

    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    df["Year"] = df["Date"].apply(lambda x: int(x[:4]))
    df["Month"] = df["Date"].apply(lambda x: int(x[5:7]))
    df["Day"] = df["Date"].apply(lambda x: int(x[8:]))

    return df


def _fetch_coffee_prices(start_date:str=None, end_date:str=None, freq:str=None):
    """
    Average Price: Coffee, 100%, Ground Roast, All Sizes (Cost per Pound/453.6 Grams) in U.S. City Average (APU0000717311) | path: /coffee-prices | freq default: M | freqs available: Q
    """
    series = fred.get_series('APU0000717311')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'Coffee 1lb']

    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]
    
    if freq.upper() == 'Q':
        df = df.set_index("Date").resample('QE').mean().reset_index()
    
    df['Coffee 1lb'] = round(df['Coffee 1lb'], 2)

    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    df["Year"] = df["Date"].apply(lambda x: int(x[:4]))
    df["Month"] = df["Date"].apply(lambda x: int(x[5:7]))
    df["Day"] = df["Date"].apply(lambda x: int(x[8:]))

    return df


def _fetch_bacon_sliced_prices(start_date:str=None, end_date:str=None, freq:str=None):
    """
    Average Price: Bacon, Sliced (Cost per Pound/453.6 Grams) in U.S. City Average (APU0000704111) | path: /bacon-prices | freq default: M | freqs available: Q
    """
    series = fred.get_series('APU0000704111')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'Bacon 1lb']

    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]
    
    if freq.upper() == 'Q':
        df = df.set_index("Date").resample('QE').mean().reset_index()
    
    df['Bacon 1lb'] = round(df['Bacon 1lb'], 2)

    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    df["Year"] = df["Date"].apply(lambda x: int(x[:4]))
    df["Month"] = df["Date"].apply(lambda x: int(x[5:7]))
    df["Day"] = df["Date"].apply(lambda x: int(x[8:]))

    return df