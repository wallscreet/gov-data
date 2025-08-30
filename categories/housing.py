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


def _fetch_median_home_prices(start_date=None, end_date=None):
    """
    Median Sales Price of Houses Sold for the United States (MSPUS) | path: /mspus | freq default: M
    """
    series = fred.get_series('MSPUS')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'Median Home Sales Price']
    
    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]
    
    df = df.set_index("Date").resample('MS').ffill().reset_index()

    df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
    df['Year'] = df['Date'].apply(lambda x: int(x[:4]))
    df['Month'] = df['Date'].apply(lambda x: int(x[5:7]))
    df['Day'] = df['Date'].apply(lambda x: int(x[8:]))

    return df


def _fetch_median_home_price_new(start_date=None, end_date=None):
    """
    Median Sales Price for New Houses Sold in the United States (MSPNHSUS) | path: /mspnus | freq default:
    """
    series = fred.get_series('MSPNHSUS')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'Median New Home Price']
    
    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]

    df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
    df['Year'] = df['Date'].apply(lambda x: int(x[:4]))
    df['Month'] = df['Date'].apply(lambda x: int(x[5:7]))
    df['Day'] = df['Date'].apply(lambda x: int(x[8:]))

    return df


def _fetch_caseshiller_home_price_index(start_date:str=None, end_date:str=None):
    """
    S&P CoreLogic Case-Shiller U.S. National Home Price Index (CSUSHPINSA) | path: /cshi | freq default:
    """
    series = fred.get_series('CSUSHPINSA')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'CSHI']
    df['CSHI'] = round(df['CSHI'], 2)
    
    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]
    
    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    df["Year"] = df["Date"].apply(lambda x: int(x[:4]))
    df["Month"] = df["Date"].apply(lambda x: int(x[5:7]))
    df["Day"] = df["Date"].apply(lambda x: int(x[8:]))

    return df


def _fetch_new_homes_ns(start_date:str=None, end_date:str=None, freq:str=None):
    """
    New Houses for Sale by Stage of Construction, Not Started (NHFSEPNTS) | path: /new-homes-us | freq default:
    """
    series = fred.get_series('NHFSEPNTS')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'New Homes NS']

    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]
    
    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    df["Year"] = df["Date"].apply(lambda x: int(x[:4]))
    df["Month"] = df["Date"].apply(lambda x: int(x[5:7]))
    df["Day"] = df["Date"].apply(lambda x: int(x[8:]))

    df['New Homes NS'] = df['New Homes NS'] * 1000

    return df


def _fetch_new_homes_uc(start_date:str=None, end_date:str=None, freq:str=None):
    """
    New Houses for Sale by Stage of Construction, Under Construction (NHFSEPUCS) | path: /new-homes-uc | freq default: 
    """
    series = fred.get_series('NHFSEPUCS')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'New Homes UC']

    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]
    
    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    df["Year"] = df["Date"].apply(lambda x: int(x[:4]))
    df["Month"] = df["Date"].apply(lambda x: int(x[5:7]))
    df["Day"] = df["Date"].apply(lambda x: int(x[8:]))

    df['New Homes UC'] = df['New Homes UC'] * 1000

    return df


def _fetch_new_homes_comp(start_date:str=None, end_date:str=None, freq:str=None):
    """
    New Houses for Sale by Stage of Construction, Completed (NHFSEPCS) | path: /new-homes-comp | freq default: M
    """
    series = fred.get_series('NHFSEPCS')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'New Homes Comp']

    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]
    
    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    df["Year"] = df["Date"].apply(lambda x: int(x[:4]))
    df["Month"] = df["Date"].apply(lambda x: int(x[5:7]))
    df["Day"] = df["Date"].apply(lambda x: int(x[8:]))

    df['New Homes Comp'] = df['New Homes Comp'] * 1000

    return df


def _fetch_new_sf_homes_for_sale(start_date:str=None, end_date:str=None, freq:str=None):
    """
    New One Family Houses for Sale in the United States (HNFSUSNSA) | path: /new-sf-homes-for-sale | freq defalt: M
    """
    series = fred.get_series('HNFSUSNSA')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'New SF Homes']

    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]
    
    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    df["Year"] = df["Date"].apply(lambda x: int(x[:4]))
    df["Month"] = df["Date"].apply(lambda x: int(x[5:7]))
    df["Day"] = df["Date"].apply(lambda x: int(x[8:]))

    df['New SF Homes'] = df['New SF Homes'] * 1000

    return df