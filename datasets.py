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


def _fetch_median_home_price_new(start_date=None, end_date=None):
    """
    Median Sales Price for New Houses Sold in the United States (MSPNHSUS)
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


def _fetch_us_birthrate(start_date:str=None, end_date:str=None, freq:str=None):
    """
    Crude Birth Rate for the United States (SPDYNCBRTINUSA). Births per 1000 people.

    Frequencies: Annual - A (default), Monthly - M
    """
    series = fred.get_series('SPDYNCBRTINUSA')
    df = series.to_frame().reset_index()
    df.columns = ['Date', 'Births Per 1000']

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


def _build_home_affordability(start_year:int=None, end_year:int=None):
    """
    Home affordabiltiy matrix by year
    """
    hoi_ref_premium = 3303
    hoi_ref_year = 2024

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

    #HOI PPI table - resampled to annual on mean
    hoi_series = fred.get_series('PCU9241269241262')
    hoi_df = hoi_series.to_frame().reset_index()
    hoi_df.columns = ['Date', 'HOI PPI']
    hoi_df['Date'] = pd.to_datetime(hoi_df['Date'])
    hoi_df.set_index('Date', inplace=True)
    hoi_df = hoi_df.resample('YE').mean()
    hoi_df.index = hoi_df.index.year
    hoi_df.reset_index(inplace=True)
    hoi_df.columns = ['Year', 'HOI PPI']
    hoi_df['HOI PPI'] = round(hoi_df['HOI PPI'], 3)

    #Estimate HOI premiums based on reference year adjusted by PPI
    hoi_ref_cpi = hoi_df.loc[hoi_df['Year'] == hoi_ref_year, 'HOI PPI'].values[0]
    hoi_df['Est HOI Premium'] = round((hoi_df['HOI PPI'] * (hoi_ref_premium / hoi_ref_cpi)), 2)

    #Merge the datasets
    merged_hoi_df = merge_on_year([hoi_df, cpi_df], how='outer')

    # anchor year where both PPI + Premium exist
    anchor_year = 1998
    premium_anchor = merged_hoi_df.loc[merged_hoi_df["Year"] == anchor_year, "Est HOI Premium"].values[0]
    cpi_anchor = merged_hoi_df.loc[merged_hoi_df["Year"] == anchor_year, "CPI"].values[0]

    # fill missing premiums before PPI begins
    mask = merged_hoi_df["Year"] < anchor_year
    merged_hoi_df.loc[mask, "Est HOI Premium"] = (
        premium_anchor * (merged_hoi_df.loc[mask, "CPI"] / cpi_anchor)
    )
    merged_hoi_df.loc[mask, "HOI PPI"] = np.nan
    # Add scaled premiums using CPI
    merged_hoi_df['Scaled Premium'] = merged_hoi_df.apply(lambda row: scale_for_inflation(cpi_df, 2024, row['Year'], row['Est HOI Premium']), axis=1)

    #Median Home Prices DF - resampled to annual as mean
    median_home_prices = fred.get_series('MSPUS')
    df_home_median_prices = median_home_prices.to_frame().reset_index()
    df_home_median_prices.columns = ['Date', 'Median Sales Price']
    df_home_median_prices['Date'] = pd.to_datetime(df_home_median_prices['Date'])
    df_home_median_prices.set_index('Date', inplace=True)
    df_home_median_prices_annual = df_home_median_prices.resample('YE').mean()
    df_home_median_prices_annual.index = df_home_median_prices_annual.index.year
    df_home_median_prices_annual.reset_index(inplace=True)
    df_home_median_prices_annual.columns = ['Year', 'Median Sales Price']

    #Median Family Income - annual series
    median_family_income = fred.get_series('MEFAINUSA646N')
    df_median_family_income =  median_family_income.to_frame().reset_index()
    df_median_family_income.columns = ['Date', 'Median Family Income']
    df_median_family_income['Date'] = pd.to_datetime(df_median_family_income['Date'])
    df_median_family_income.set_index('Date', inplace=True)
    df_median_family_income.index = df_median_family_income.index.year
    df_median_family_income.reset_index(inplace=True)
    df_median_family_income.columns = ['Year', 'Median Family Income']

    #30Yr Mortgage Rates - resampled to annual as mean
    mtg30 = fred.get_series('MORTGAGE30US')
    df_mtg30 = mtg30.to_frame().reset_index()
    df_mtg30.columns = ['Date', '30yr Mtg Rate']
    df_mtg30['Date'] = pd.to_datetime(df_mtg30['Date'])
    df_mtg30.set_index('Date', inplace=True)
    df_mtg30 = df_mtg30.resample('YE').mean()
    df_mtg30.index = df_mtg30.index.year
    df_mtg30.reset_index(inplace=True)
    df_mtg30.columns = ['Year', '30yr Mtg Rate']
    df_mtg30['30yr Mtg Rate'] = round(df_mtg30['30yr Mtg Rate'], 3)

    #Merge datasets and add customer features
    cdf = merge_on_year([merged_hoi_df, df_home_median_prices_annual, df_median_family_income, df_mtg30])
    cdf['Avg Loan Amount'] = cdf['Median Sales Price'] * .8
    cdf['Mtg PI Monthly'] = cdf.apply(lambda row: calc_mtg_pi_payment(row['Avg Loan Amount'], row['30yr Mtg Rate']), axis=1).round(2)
    cdf['Mtg PI Annual'] = round(cdf['Mtg PI Monthly'] * 12, 2)
    cdf['Mtg PII Annual'] = round(cdf['Mtg PI Annual'] + cdf['Scaled Premium'], 2)
    cdf['Mtg PII Monthly'] = round((cdf['Mtg PI Annual'] / 12) + (cdf['Scaled Premium'] / 12), 2)
    cdf['Mtg Ratio'] = round(cdf['Mtg PII Annual'] / cdf['Median Family Income'], 3)

    #Filter by year(s)
    if start_year is not None:
        cdf = cdf[cdf['Year'] >= start_year]
    if end_year is not None:
        cdf = cdf[cdf['Year'] <= end_year]

    return cdf


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


def _fetch_new_homes_ns(start_date:str=None, end_date:str=None, freq:str=None):
    """
    New Houses for Sale by Stage of Construction, Not Started (NHFSEPNTS)
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
    New Houses for Sale by Stage of Construction, Under Construction (NHFSEPUCS)
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
    New Houses for Sale by Stage of Construction, Completed (NHFSEPCS)
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


def _fetch_birth_death_data(
    start_year: int | None = None,
    end_year: int | None = None,
    race: str | None = None
) -> pd.DataFrame:
    """
    Births and Deaths by Race/Ethnicity (CDC Exfil)
    """
    race_map = {
        "all": "All Races/Ethnicities",
        "white": "Non-Hispanic White",
        "black": "Non-Hispanic Black",
        "hispanic": "Hispanic",
    }

    df = pd.read_csv("static_datasets/us_births_deaths.csv")

    if start_year is not None:
        df = df[df["Year"] >= start_year]
    if end_year is not None:
        df = df[df["Year"] <= end_year]
    if race is not None:
        race_key = race.lower()
        if race_key not in race_map:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid race value '{race}'. Valid options: {list(race_map.keys())}"
            )
        df = df[df["RaceEthnicity"] == race_map[race_key]]

    df["Year"] = df["Year"].astype(int)

    return df
