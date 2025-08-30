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


def _fetch_30yr_mortgage_rates(start_date=None, end_date=None, freq:str=None):
    """
    30-Year Fixed Rate Mortgage Average in the United States (MORTGAGE30US)
    
    Frequencies: Weekly - W (default), Monthly - M
    """
    series = fred.get_series('MORTGAGE30US')
    df = series.to_frame().reset_index()
    df.columns = ['Date', '30yr Mortgage Rate']
    
    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]
    
    if freq == 'M':
        df = df.set_index("Date").resample('MS').mean().reset_index()
    
    df['30yr Mortgage Rate'] = round(df['30yr Mortgage Rate'], 3)

    df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
    df['Year'] = df['Date'].apply(lambda x: int(x[:4]))
    df['Month'] = df['Date'].apply(lambda x: int(x[5:7]))
    df['Day'] = df['Date'].apply(lambda x: int(x[8:]))

    return df


def _fetch_15yr_mortgage_rates(start_date=None, end_date=None, freq:str=None):
    """
    15-Year Fixed Rate Mortgage Average in the United States (MORTGAGE15US)
    
    Frequencies: Weekly - W (default), Monthly - M
    """
    series = fred.get_series('MORTGAGE15US')
    df = series.to_frame().reset_index()
    df.columns = ['Date', '15yr Mortgage Rate']
    
    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]
    
    if freq == 'M':
        df = df.set_index("Date").resample('MS').mean().reset_index()
    
    df['15yr Mortgage Rate'] = round(df['15yr Mortgage Rate'], 3)

    df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
    df['Year'] = df['Date'].apply(lambda x: int(x[:4]))
    df['Month'] = df['Date'].apply(lambda x: int(x[5:7]))
    df['Day'] = df['Date'].apply(lambda x: int(x[8:]))

    return df


def _fetch_sofr(start_date:str=None, end_date:str=None, freq:str=None):
    """
    Secured Overnight Financing Rate (SOFR)

    Frequencies: Daily - D (default), Weekly - W, Monthly - W, Quarterly - M
    Default period aggregation is mean.
    """
    series = fred.get_series('SOFR')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'SOFR']
    
    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]
    
    if freq == 'W':
        df = df.set_index("Date").resample('W').mean().reset_index()
    if freq == 'M':
        df = df.set_index("Date").resample('MS').mean().reset_index()
    if freq == 'Q':
        df = df.set_index("Date").resample('QS').mean().reset_index()

    df['SOFR'] = round(df['SOFR'], 3)
    
    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    df["Year"] = df["Date"].apply(lambda x: int(x[:4]))
    df["Month"] = df["Date"].apply(lambda x: int(x[5:7]))
    df["Day"] = df["Date"].apply(lambda x: int(x[8:]))

    return df


def _fetch_fed_funds_rate(start_date:str=None, end_date:str=None, freq:str=None):
    """
    Federal Funds Effective Rate (FEDFUNDS)
    """
    series = fred.get_series('FEDFUNDS')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'Fed Funds Rate']

    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]
    
    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    df["Year"] = df["Date"].apply(lambda x: int(x[:4]))
    df["Month"] = df["Date"].apply(lambda x: int(x[5:7]))
    df["Day"] = df["Date"].apply(lambda x: int(x[8:]))

    return df