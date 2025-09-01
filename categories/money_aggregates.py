from fredapi import Fred
from dotenv import load_dotenv
import os

load_dotenv()

FRED_API_KEY = os.getenv("FRED_API_KEY")

fred = Fred(api_key=FRED_API_KEY)


def _fetch_m2_supply(start_date:str=None, end_date:str=None, freq:str=None):
    """
    M2 (M2SL)

    Frequencies: Monthly - M (default), Quarterly - Q, Annual - A
    Default period aggregation is mean.
    """
    series = fred.get_series('M2SL')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'M2 Supply']
    
    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]
    
    if freq == "Q":
        df = df.set_index('Date').resample('QS').mean().reset_index()
    if freq == "A":
        df = df.set_index('Date').resample('A').mean().reset_index()
    
    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    df["Year"] = df["Date"].apply(lambda x: int(x[:4]))
    df["Month"] = df["Date"].apply(lambda x: int(x[5:7]))
    df["Day"] = df["Date"].apply(lambda x: int(x[8:]))

    df['M2 Supply'] = df['M2 Supply'] * 1000000000

    return df


def _fetch_m2_velocity(start_date:str=None, end_date:str=None, freq:str=None):
    """
    Velocity of M2 Money Stock (M2V)

    Frequencies: Quarterly - Q (default), Monthly - M
    """
    series = fred.get_series('M2V')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'M2 Velocity']
    
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

    return df