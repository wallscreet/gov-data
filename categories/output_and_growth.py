import pandas as pd
from fredapi import Fred
from dotenv import load_dotenv
import os

load_dotenv()

FRED_API_KEY = os.getenv("FRED_API_KEY")

fred = Fred(api_key=FRED_API_KEY)


def _fetch_gdp(start_date:str=None, end_date:str=None, freq:str=None):
    """
    Gross Domestic Product (GDP)

    Frequencies: Quarterly - Q (default), Monthly - M
    """
    series = fred.get_series('GDP')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'GDP']
    df['Date'] = pd.to_datetime(df['Date'])
    df['Date'] = df['Date'] - pd.Timedelta(days=1)
    
    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]
    
    if freq == 'M':
        df = df.set_index("Date").resample('MS').ffill().reset_index()
    
    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    df["Year"] = df["Date"].apply(lambda x: int(x[:4]))
    df["Month"] = df["Date"].apply(lambda x: int(x[5:7]))
    df["Day"] = df["Date"].apply(lambda x: int(x[8:]))

    df['GDP'] = df['GDP'] * 1000000000

    return df