import pandas as pd
pd.set_option('display.max_columns', None)
from fredapi import Fred
from dotenv import load_dotenv
import os
import json

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
    df["Year"] = df["Date"].apply(lambda x: int(x[:4]))
    df["Month"] = df["Date"].apply(lambda x: int(x[5:7]))
    df["Day"] = df["Date"].apply(lambda x: int(x[8:]))

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
    df["Year"] = df["Date"].apply(lambda x: int(x[:4]))
    df["Month"] = df["Date"].apply(lambda x: int(x[5:7]))
    df["Day"] = df["Date"].apply(lambda x: int(x[8:]))

    df['PCE'] = df['PCE'] * 1000000000

    return df


def _fetch_us_households(start_date=None, end_date=None):
    """
    Total Households (TTLHH)
    """
    series = fred.get_series('TTLHH')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'US Households']

    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]

    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    df["Year"] = df["Date"].apply(lambda x: int(x[:4]))
    df["Month"] = df["Date"].apply(lambda x: int(x[5:7]))
    df["Day"] = df["Date"].apply(lambda x: int(x[8:]))
    
    df['US Households'] = (df['US Households'] * 1000)
    
    return df


def _fetch_us_population(start_date=None, end_date=None):
    """
    Population (POPTHM)
    """
    series = fred.get_series('POPTHM')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'US Population']

    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]
    
    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    df["Year"] = df["Date"].apply(lambda x: int(x[:4]))
    df["Month"] = df["Date"].apply(lambda x: int(x[5:7]))
    df["Day"] = df["Date"].apply(lambda x: int(x[8:]))

    df['US Population']= df['US Population'] * 1000

    return df


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


def _fetch_30yr_mortgage_rates(start_date=None, end_date=None):
    """
    30-Year Fixed Rate Mortgage Average in the United States (MORTGAGE30US)
    Weekly series resampled to monthly mean.
    """
    series = fred.get_series('MORTGAGE30US')
    df = series.to_frame().reset_index()
    df.columns = ['Date', '30yr Mortgage Rate']
    
    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]
    
    df = df.set_index("Date").resample('MS').mean().reset_index()
    df['30yr Mortgage Rate'] = round(df['30yr Mortgage Rate'], 2)

    df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
    df['Year'] = df['Date'].apply(lambda x: int(x[:4]))
    df['Month'] = df['Date'].apply(lambda x: int(x[5:7]))
    df['Day'] = df['Date'].apply(lambda x: int(x[8:]))

    return df


def _fetch_15yr_mortgage_rates(start_date=None, end_date=None):
    """
    15-Year Fixed Rate Mortgage Average in the United States (MORTGAGE15US)
    Weekly series resampled to monthly mean.
    """
    series = fred.get_series('MORTGAGE15US')
    df = series.to_frame().reset_index()
    df.columns = ['Date', '15yr Mortgage Rate']
    
    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]
    
    df = df.set_index("Date").resample('MS').mean().reset_index()
    df['15yr Mortgage Rate'] = round(df['15yr Mortgage Rate'], 2)

    df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
    df['Year'] = df['Date'].apply(lambda x: int(x[:4]))
    df['Month'] = df['Date'].apply(lambda x: int(x[5:7]))
    df['Day'] = df['Date'].apply(lambda x: int(x[8:]))

    return df


def _fetch_real_disposable_personal_income(start_date=None, end_date=None):
    """
    Real Disposable Personal Income (DSPI)
    """
    series = fred.get_series('DSPI')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'RDPI']
    
    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]
    
    df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
    df['Year'] = df['Date'].apply(lambda x: int(x[:4]))
    df['Month'] = df['Date'].apply(lambda x: int(x[5:7]))
    df['Day'] = df['Date'].apply(lambda x: int(x[8:]))

    df['RDPI'] = df['RDPI'] * 1000000000

    return df


def _fetch_median_home_prices(start_date=None, end_date=None):
    """
    Median Sales Price of Houses Sold for the United States (MSPUS)
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


def _fetch_caseshiller_home_price_index(start_date:str=None, end_date:str=None):
    """
    S&P CoreLogic Case-Shiller U.S. National Home Price Index (CSUSHPINSA)
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


def _fetch_houshold_ops_spend(start_date:str=None, end_date:str=None):
    """
    Expenditures: Household Operations: All Consumer Units (CXUHHOPERLB0101M)
    """
    series = fred.get_series('CXUHHOPERLB0101M')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'Household Ops Annual']
    
    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]
    
    df = df.set_index("Date").resample('MS').ffill().reset_index()
    
    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    df["Year"] = df["Date"].apply(lambda x: int(x[:4]))
    df["Month"] = df["Date"].apply(lambda x: int(x[5:7]))
    df["Day"] = df["Date"].apply(lambda x: int(x[8:]))

    return df


def _fetch_used_car_prices(start_date:str=None, end_date:str=None):
    """
    CPI Used Cars and Trucks (CUSR0000SETA02). Prices calculated based on CPI index applied to reference year and price
    """
    ref_year = 2024
    ref_price = 28472

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
    df['Est Avg Used Car Price'] = round((df['Used Auto CPI'] * (ref_price / ref_cpi)),2)

    return df


def _fetch_new_car_prices(start_date:str=None, end_date:str=None):
    """
    CPI New Cars and Trucks (CUUR0000SETA01). Prices calculated based on CPI index applied to reference year and price
    """
    ref_year = 2024
    ref_price = 48397

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
    df['Est Avg New Car Price'] = round((df['New Auto CPI'] * (ref_price / ref_cpi)), 2)

    return df


def _fetch_vehicle_ins_premiums(start_date:str=None, end_date:str=None):
    """
    Expenditures: Vehicle Insurance: All Consumer Units (CXU500110LB0101M)
    """
    series = fred.get_series('CXU500110lB0101M')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'Vehicle Ins Annual']
    
    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]
    
    df = df.set_index("Date").resample('MS').ffill().reset_index()
    
    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    df["Year"] = df["Date"].apply(lambda x: int(x[:4]))
    df["Month"] = df["Date"].apply(lambda x: int(x[5:7]))
    df["Day"] = df["Date"].apply(lambda x: int(x[8:]))

    return df


def _fetch_pce_healthcare(start_date:str=None, end_date:str=None):
    """
    PCE Services: Healthcare (DHLCRC1Q027SBEA).
    """
    series = fred.get_series('DHLCRC1Q027SBEA')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'PCE Healthcare']
    
    if start_date is not None:
        df = df[df['Date'] >= start_date]
    if end_date is not None:
        df = df[df['Date'] <= end_date]
    
    df = df.set_index("Date").resample('MS').ffill().reset_index()
    
    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
    df["Year"] = df["Date"].apply(lambda x: int(x[:4]))
    df["Month"] = df["Date"].apply(lambda x: int(x[5:7]))
    df["Day"] = df["Date"].apply(lambda x: int(x[8:]))
    
    df['PCE Healthcare'] = df['PCE Healthcare'] * 1000000000

    return df

