from http.client import HTTPException
import pandas as pd
from fredapi import Fred
from dotenv import load_dotenv
import os
import json
from utils import merge_on_date
import numpy as np

load_dotenv()

FRED_API_KEY = os.getenv("FRED_API_KEY")

fred = Fred(api_key=FRED_API_KEY)


def _fetch_cpi(start_date:str=None, end_date:str=None):
    """
    Consumer Price Index for All Urban Consumers: All Items in U.S. City Average (CPIAUCSL)
    """
    series = fred.get_series('CPIAUCSL')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'CPI']
    
    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]
    
    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    # df["Year"] = df["Date"].apply(lambda x: int(x[:4]))
    # df["Month"] = df["Date"].apply(lambda x: int(x[5:7]))
    # df["Day"] = df["Date"].apply(lambda x: int(x[8:]))

    return df


def _fetch_pce(start_date=None, end_date=None):
    """
    Personal Consumption Expenditures (PCE)
    """
    series = fred.get_series('PCE')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'PCE']
    
    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]

    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    # df["Year"] = df["Date"].apply(lambda x: int(x[:4]))
    # df["Month"] = df["Date"].apply(lambda x: int(x[5:7]))
    # df["Day"] = df["Date"].apply(lambda x: int(x[8:]))

    df['PCE'] = df['PCE'] * 1000000000

    return df


def _fetch_used_car_prices(start_date:str=None, end_date:str=None):
    """
    CPI Used Cars and Trucks (CUSR0000SETA02). Prices calculated based on CPI index applied to reference year and price
    """
    ref_auto_cpi = 185.660
    ref_price = 28472

    # Used Auto CPI
    used_auto_series = fred.get_series('CUSR0000SETA02')
    used_auto_df = used_auto_series.to_frame().reset_index()
    used_auto_df.columns = ['Date', 'Used Auto CPI']

    # CPI
    _cpi_series = fred.get_series('CPIAUCSL')
    _cpi_df = _cpi_series.to_frame().reset_index()
    _cpi_df.columns = ['Date', 'CPI']

    used_merged = used_auto_df.merge(_cpi_df, 'inner', 'Date')

    used_merged['Used Auto Price Real'] = round(used_merged['Used Auto CPI'] * (ref_price / ref_auto_cpi),2)
    ref_cpi = used_merged['CPI'].iloc[-1]
    used_merged['Used Auto Price Nominal'] = round(used_merged['Used Auto Price Real'] * (used_merged['CPI'] / ref_cpi), 2)

    if start_date is not None:
        used_merged = used_merged[used_merged['Date'] >= start_date]
    if end_date is not None:
        used_merged = used_merged[used_merged['Date'] <= end_date]
    
    used_merged['Date'] = used_merged['Date'].dt.strftime('%Y-%m-%d')

    drop_cols = ['CPI']
    
    return used_merged.drop(columns=drop_cols)


def _fetch_new_car_prices(start_date:str=None, end_date:str=None):
    """
    CPI New Cars and Trucks (CUUR0000SETA01). Prices calculated based on CPI index applied to reference year and price
    """
    ref_auto_cpi = 178.001
    ref_price = 48397

    # New Auto CPI
    new_auto_series = fred.get_series('CUUR0000SETA01')
    new_auto_df = new_auto_series.to_frame().reset_index()
    new_auto_df.columns = ['Date', 'New Auto CPI']

    # CPI
    _cpi_series = fred.get_series('CPIAUCSL')
    _cpi_df = _cpi_series.to_frame().reset_index()
    _cpi_df.columns = ['Date', 'CPI']

    new_merged = new_auto_df.merge(_cpi_df, 'inner', 'Date')
    new_merged['New Auto Price Real'] = round(new_merged['New Auto CPI'] * (ref_price / ref_auto_cpi),2)
    ref_cpi = new_merged['CPI'].iloc[-1]
    new_merged['New Auto Price Nominal'] = round(new_merged['New Auto Price Real'] * (new_merged['CPI'] / ref_cpi), 2)

    if start_date is not None:
        new_merged = new_merged[new_merged['Date'] >= start_date]
    if end_date is not None:
        new_merged = new_merged[new_merged['Date'] <= end_date]
    
    new_merged['Date'] = new_merged['Date'].dt.strftime('%Y-%m-%d')

    drop_cols = ['CPI']
    
    return new_merged.drop(columns=drop_cols)


def _fetch_all_car_prices(start_date:str=None, end_date:str=None):
    """
    Merged dataset with New Car CPI (CUUR0000SETA01) and Used Car CPI (CUSR0000SETA02). Prices calculated based on CPI indices for New and Used autos applied to reference years and prices
    """
    used_df = _fetch_used_car_prices(start_date=start_date, end_date=end_date)
    new_df = _fetch_new_car_prices(start_date=start_date, end_date=end_date)

    df = merge_on_date([new_df, used_df])
    
    return df