#import pandas as pd
from fredapi import Fred
from dotenv import load_dotenv
import os

# import numpy as np

load_dotenv()

FRED_API_KEY = os.getenv("FRED_API_KEY")

fred = Fred(api_key=FRED_API_KEY)


def _fetch_median_family_income(start_date=None, end_date=None):
    """
    Median Annual Family Income in the United States (MEFAINUSA646N)
    """
    series = fred.get_series('MEFAINUSA646N')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'Median Family Income']
    
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


def _fetch_unrate(start_date:str=None, end_date:str=None, freq:str=None):
    """
    Unemployment Rate (UNRATE)

    Frequencies: Monthly - M (default), Quarterly - Q
    Default period aggregation is mean.
    """
    series = fred.get_series('UNRATE')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'Unrate']
    
    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]
    
    if freq == 'Q':
        df = df.set_index('Date').resample('QS').mean().reset_index()
    
    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    df["Year"] = df["Date"].apply(lambda x: int(x[:4]))
    df["Month"] = df["Date"].apply(lambda x: int(x[5:7]))
    df["Day"] = df["Date"].apply(lambda x: int(x[8:]))

    df['Unrate'] = round(df['Unrate'], 2)

    return df


def _fetch_unemployment_level(start_date:str=None, end_date:str=None, freq:str=None):
    """
    Unemployment Level (UNEMPLOY)
    """
    series = fred.get_series('unemploy')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'Unemployed']

    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]
    
    if freq.upper() == 'Q':
        df = df.set_index('Date').resample('Q').mean()

    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    df["Year"] = df["Date"].apply(lambda x: int(x[:4]))
    df["Month"] = df["Date"].apply(lambda x: int(x[5:7]))
    df["Day"] = df["Date"].apply(lambda x: int(x[8:]))

    df['Unemployed'] = df['Unemployed'] * 1000

    return df


def _fetch_job_openings(start_date:str=None, end_date:str=None, freq:str=None):
    """
    Job Openings: Total Nonfarm (JTSJOL)
    """
    series = fred.get_series('JTSJOL')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'Job Openings']

    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]
    
    if freq.upper() == 'Q':
        df = df.set_index('Date').resample('Q').mean()

    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    df["Year"] = df["Date"].apply(lambda x: int(x[:4]))
    df["Month"] = df["Date"].apply(lambda x: int(x[5:7]))
    df["Day"] = df["Date"].apply(lambda x: int(x[8:]))

    df['Job Openings'] = df['Job Openings'] * 1000

    return df