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
    ref_year = 2024
    ref_price = 28472

    #CPI table - resampled to annual on mean
    cpi = fred.get_series('CPIAUCSL')
    cpi_df = cpi.to_frame().reset_index()
    cpi_df.columns = ['Date', 'CPI']
    cpi_df['Date'] = pd.to_datetime(cpi_df['Date'])
    cpi_df.set_index('Date', inplace=True)
    cpi_df = cpi_df.resample('YE').max()
    cpi_df.index = cpi_df.index.year
    cpi_df.reset_index(inplace=True)
    cpi_df.columns = ['Year', 'CPI']

    series = fred.get_series('CUSR0000SETA02')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'Used Auto CPI']
    
    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]
    
    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    df["Year"] = df["Date"].apply(lambda x: int(x[:4]))
    df["Month"] = df["Date"].apply(lambda x: int(x[5:7]))
    df["Day"] = df["Date"].apply(lambda x: int(x[8:]))

    ref_cpi = df.loc[df['Year'] == ref_year, 'Used Auto CPI'].values[0]
    df['Est Avg Used Car Price Real'] = round((df['Used Auto CPI'] * (ref_price / ref_cpi)),2)
    
    df['Est Avg Used Car Price'] = df.apply(lambda row: scale_for_inflation(cpi_df, ref_year, row['Year'], row['Est Avg Used Car Price Real']), axis=1)

    return df


def _fetch_new_car_prices(start_date:str=None, end_date:str=None):
    """
    CPI New Cars and Trucks (CUUR0000SETA01). Prices calculated based on CPI index applied to reference year and price
    """
    ref_year = 2024
    ref_price = 48397

    #CPI table - resampled to annual on mean
    cpi = fred.get_series('CPIAUCSL')
    cpi_df = cpi.to_frame().reset_index()
    cpi_df.columns = ['Date', 'CPI']
    cpi_df['Date'] = pd.to_datetime(cpi_df['Date'])
    cpi_df.set_index('Date', inplace=True)
    cpi_df = cpi_df.resample('YE').max()
    cpi_df.index = cpi_df.index.year
    cpi_df.reset_index(inplace=True)
    cpi_df.columns = ['Year', 'CPI']

    series = fred.get_series('CUUR0000SETA01')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'New Auto CPI']
    
    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]
    
    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    df["Year"] = df["Date"].apply(lambda x: int(x[:4]))
    df["Month"] = df["Date"].apply(lambda x: int(x[5:7]))
    df["Day"] = df["Date"].apply(lambda x: int(x[8:]))

    ref_cpi = df.loc[df['Year'] == ref_year, 'New Auto CPI'].values[0]
    df['Est Avg New Car Price Real'] = round((df['New Auto CPI'] * (ref_price / ref_cpi)), 2)

    df['Est Avg New Car Price'] = df.apply(lambda row: scale_for_inflation(cpi_df, ref_year, row['Year'], row['Est Avg New Car Price Real']), axis=1)

    return df