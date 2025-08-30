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


def _fetch_us_households(start_date=None, end_date=None):
    """
    Total Households (TTLHH) | path: /households | freq default:
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
    Population (POPTHM) | path: /population | freq default:
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


def _fetch_us_birthrate(start_date:str=None, end_date:str=None, freq:str=None):
    """
    Crude Birth Rate for the United States (SPDYNCBRTINUSA). Births per 1000 people. | path: /us-birthrate | freq default: M
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


def _fetch_birth_death_data(start_year: int | None = None, end_year: int | None = None, race: str | None = None) -> pd.DataFrame:
    """
    Births and Deaths by Race/Ethnicity (CDC Exfil) | path: /us-births-deaths-by-race | freq default: A
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