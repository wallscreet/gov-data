from fredapi import Fred
from dotenv import load_dotenv
import os

load_dotenv()

FRED_API_KEY = os.getenv("FRED_API_KEY")

fred = Fred(api_key=FRED_API_KEY)


def _fetch_dq_credit_cards(start_date:str=None, end_date:str=None, freq:str=None):
    """
    Delinquency Rate on Credit Card Loans, All Commercial Banks (DRCCLACBS) | path: /dq-credit-cards | freq default: Q | freq available: M | range: 1991-current
    """
    series = fred.get_series('DRCCLACBS')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'DQ Percent']

    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]
    
    if freq.upper() == 'M':
        df = df.set_index("Date").resample('MS').ffill().reset_index()

    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    df["Year"] = df["Date"].apply(lambda x: int(x[:4]))
    df["Month"] = df["Date"].apply(lambda x: int(x[5:7]))
    df["Day"] = df["Date"].apply(lambda x: int(x[8:]))

    return df


def _fetch_dq_consumer_loans(start_date:str=None, end_date:str=None, freq:str=None):
    """
    Delinquency Rate on Consumer Loans, All Commercial Banks (DRCLACBS) | path: /dq-consumer-loans | freq default: Q | freq available: M | range: 1987-current
    """
    series = fred.get_series('DRCLACBS')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'DQ Percent']

    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]
    
    if freq.upper() == 'M':
        df = df.set_index("Date").resample('MS').ffill().reset_index()

    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    df["Year"] = df["Date"].apply(lambda x: int(x[:4]))
    df["Month"] = df["Date"].apply(lambda x: int(x[5:7]))
    df["Day"] = df["Date"].apply(lambda x: int(x[8:]))

    return df


def _fetch_dq_sfr_mortgages(start_date:str=None, end_date:str=None, freq:str=None):
    """
    Delinquency Rate on Single-Family Residential Mortgages, Booked in Domestic Offices, All Commercial Banks (DRSFRMACBS) | path: /dq-sfr-mtg | freq default: Q | freq available: M | range: 1991-current
    """
    series = fred.get_series('DRSFRMACBS')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'DQ Percent']

    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]
    
    if freq.upper() == 'M':
        df = df.set_index("Date").resample('MS').ffill().reset_index()

    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    df["Year"] = df["Date"].apply(lambda x: int(x[:4]))
    df["Month"] = df["Date"].apply(lambda x: int(x[5:7]))
    df["Day"] = df["Date"].apply(lambda x: int(x[8:]))

    return df


def _fetch_dq_all_loans(start_date:str=None, end_date:str=None, freq:str=None):
    """
    Delinquency Rate on All Loans, All Commercial Banks (DRALACBS) | path: /dq-all-loans | freq default: Q | freq available: M | range: 1985-current
    """
    series = fred.get_series('DRALACBS')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'DQ Percent']

    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]
    
    if freq.upper() == 'M':
        df = df.set_index("Date").resample('MS').ffill().reset_index()

    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    df["Year"] = df["Date"].apply(lambda x: int(x[:4]))
    df["Month"] = df["Date"].apply(lambda x: int(x[5:7]))
    df["Day"] = df["Date"].apply(lambda x: int(x[8:]))

    return df