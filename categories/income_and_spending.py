# from http.client import HTTPException
import pandas as pd
from fredapi import Fred
from dotenv import load_dotenv
import os
# import json
from utils import merge_on_year, scale_for_inflation, calc_mtg_pi_payment
import numpy as np

load_dotenv()

FRED_API_KEY = os.getenv("FRED_API_KEY")

fred = Fred(api_key=FRED_API_KEY)


def _fetch_real_disposable_personal_income(start_date=None, end_date=None):
    """
    Real Disposable Personal Income (DSPI) | path: /rdpi | default freq:
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


def _fetch_vehicle_ins_premiums(start_date:str=None, end_date:str=None):
    """
    Expenditures: Vehicle Insurance: All Consumer Units (CXU500110LB0101M) | path: /vehicle-insurance | default freq: M 
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
    PCE Services: Healthcare (DHLCRC1Q027SBEA) | path: /pce-healthcare | default freq: M
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


def _fetch_houshold_ops_spend(start_date:str=None, end_date:str=None):
    """
    Expenditures: Household Operations: All Consumer Units (CXUHHOPERLB0101M) | path: /hh-ops | default freq: M
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


def _fetch_build_home_affordability(start_year:int=None, end_year:int=None):
    """
    Home affordabiltiy matrix by year | path: /home-affordability | default freq: A
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