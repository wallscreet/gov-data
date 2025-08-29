from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
import pandas as pd
from functools import reduce
from utils import sanitize_for_json

from datasets import (
    _fetch_us_population,
    _fetch_cpi,
    _fetch_pce,
    _fetch_us_households,
    _fetch_median_family_income,
    _fetch_30yr_mortgage_rates,
    _fetch_15yr_mortgage_rates,
    _fetch_real_disposable_personal_income,
    _fetch_median_home_prices,
    _fetch_caseshiller_home_price_index,
    _fetch_houshold_ops_spend,
    _fetch_used_car_prices,
    _fetch_new_car_prices,
    _fetch_vehicle_ins_premiums,
    _fetch_pce_healthcare,
    _fetch_unrate,
    _fetch_m2_supply,
    _fetch_m2_velocity,
    _fetch_gdp,
    _fetch_sofr,
    _fetch_us_birthrate,
    _build_home_affordability,
    _fetch_unemployment_level,
    _fetch_job_openings,
    _fetch_fed_funds_rate,
    _fetch_median_home_price_new,
    _fetch_new_homes_ns,
    _fetch_new_homes_uc,
    _fetch_new_homes_comp,
    _fetch_birth_death_data,
)

app = FastAPI(title="GovData API", version="0.1.0")


datasets = {
    "cpi": _fetch_cpi,
    "pce": _fetch_pce,
    "households": _fetch_us_households,
    "population": _fetch_us_population,
    "median-family-income": _fetch_median_family_income,
    "mortgage-30yr": _fetch_30yr_mortgage_rates,
    "mortgage-15yr": _fetch_15yr_mortgage_rates,
    "rdpi": _fetch_real_disposable_personal_income,
    "mspus": _fetch_median_home_prices,
    "mspnus": _fetch_median_home_price_new,
    "cshi": _fetch_caseshiller_home_price_index,
    "hh-ops": _fetch_houshold_ops_spend,
    "used-cars": _fetch_used_car_prices,
    "new-cars": _fetch_new_car_prices,
    "vehicle-insurance": _fetch_vehicle_ins_premiums,
    "pce-healthcare": _fetch_pce_healthcare,
    "unrate": _fetch_unrate,
    "m2-supply": _fetch_m2_supply,
    "m2-velocity": _fetch_m2_velocity,
    "gdp": _fetch_gdp,
    "sofr": _fetch_sofr,
    "us-birthrate": _fetch_us_birthrate,
    "home-affordability": _build_home_affordability,
    "unemployment-level": _fetch_unemployment_level,
    "job-openings": _fetch_job_openings,
    "fed-funds-rate": _fetch_fed_funds_rate,
    "new-homes-ns": _fetch_new_homes_ns,
    "new-homes-uc": _fetch_new_homes_uc,
    "new-homes-comp": _fetch_new_homes_comp,
    "us-births-deaths-by-race": _fetch_birth_death_data,
}


@app.get("/")
def root():
    return {"message": "GovData API", "available_datasets": list(datasets.keys())}


@app.get("/dataset/{dataset_name}")
def get_dataset(
    dataset_name: str,
    start: int | None = Query(None),
    end: int | None = Query(None),
):
    
    if dataset_name not in datasets:
        raise HTTPException(status_code=404, detail="Dataset not found")

    try:
        df: pd.DataFrame = datasets[dataset_name]()

        # Apply filters
        if start is not None:
            df = df[df["Year"] >= start]
        if end is not None:
            df = df[df["Year"] <= end]
        
        return JSONResponse(sanitize_for_json(df))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/merge")
def merge_datasets(
    names: str = Query(..., description="Comma-separated dataset names"),
    how: str = Query("inner", description="Merge type: inner, outer, left, right"),
):
    dataset_list = names.split(",")

    dfs = []
    for name in dataset_list:
        if name not in datasets:
            raise HTTPException(status_code=404, detail=f"Dataset {name} not found")
        
        dfs.append(datasets[name]())

    # Merge on Year
    merged_df = reduce(lambda left, right: pd.merge(left, right, on="Year", how=how), dfs)

    return JSONResponse(content=merged_df.to_dict(orient="records"))


@app.get("/cpi")
def get_cpi(
    start_date: str | None = Query(None, description="Filter start date (YYYY-MM-DD)"),
    end_date: str | None = Query(None, description="Filter end date (YYYY-MM-DD)"),
):
    """Consumer Price Index for All Urban Consumers (CPIAUCSL)."""
    try: 
        df:pd.DataFrame = _fetch_cpi(start_date=start_date, end_date=end_date)

        return JSONResponse(content=sanitize_for_json(df))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/pce")
def get_pce(
    start_date: str | None = Query(None, description="Filter start date (YYYY-MM-DD)"),
    end_date: str | None = Query(None, description="Filter end date (YYYY-MM-DD)")
):
    """Consumer Price Index for All Urban Consumers (CPIAUCSL)."""
    try: 
        df:pd.DataFrame = _fetch_pce(start_date=start_date, end_date=end_date) 

        return JSONResponse(content=sanitize_for_json(df))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/households")
def get_households(
    start_date: str | None = Query(None, description="Filter start date (YYYY-MM-DD)"),
    end_date: str | None = Query(None, description="Filter end date (YYYY-MM-DD)")
):
    """Total Households (TTLHH)"""
    try: 
        df:pd.DataFrame = _fetch_us_households(start_date=start_date, end_date=end_date)

        return JSONResponse(content=sanitize_for_json(df))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/population")
def get_population(
    start_date: str | None = Query(None, description="Filter start date (YYYY-MM-DD)"),
    end_date: str | None = Query(None, description="Filter end date (YYYY-MM-DD)")
):
    """Total Households (TTLHH)"""
    try: 
        df:pd.DataFrame = _fetch_us_population(start_date=start_date, end_date=end_date)

        return JSONResponse(content=sanitize_for_json(df))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/median-family-income")
def get_median_income(
    start_date: str | None = Query(None, description="Filter start date (YYYY-MM-DD)"),
    end_date: str | None = Query(None, description="Filter end date (YYYY-MM-DD)")
):
    """
    Median Annual Family Income in the United States (MEFAINUSA646N)
    """
    try: 
        df:pd.DataFrame = _fetch_median_family_income(start_date=start_date, end_date=end_date)

        return JSONResponse(content=sanitize_for_json(df))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/mortgage-30yr")
def get_30yr_mortgage_rates(
    start_date: str | None = Query(None, description="Filter start date (YYYY-MM-DD)"),
    end_date: str | None = Query(None, description="Filter end date (YYYY-MM-DD)"),
    freq:str = Query('A', description="Frequency period")
):
    """
    30-Year Fixed Rate Mortgage Average in the United States (MORTGAGE30US)
    """
    try: 
        df:pd.DataFrame = _fetch_30yr_mortgage_rates(start_date=start_date, end_date=end_date, freq=freq)

        return JSONResponse(content=sanitize_for_json(df))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/mortgage-15yr")
def get_15yr_mortgage_rates(
    start_date: str | None = Query(None, description="Filter start date (YYYY-MM-DD)"),
    end_date: str | None = Query(None, description="Filter end date (YYYY-MM-DD)"),
    freq:str = Query('A', description="Frequency period")
):
    """
    15-Year Fixed Rate Mortgage Average in the United States (MORTGAGE15US)
    """
    try: 
        df:pd.DataFrame = _fetch_15yr_mortgage_rates(start_date=start_date, end_date=end_date, freq=freq)

        return JSONResponse(content=sanitize_for_json(df))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/rdpi")
def get_rdpi(
    start_date: str | None = Query(None, description="Filter start date (YYYY-MM-DD)"),
    end_date: str | None = Query(None, description="Filter end date (YYYY-MM-DD)")
):
    """
    Real Disposable Personal Income (DSPI)
    """
    try: 
        df:pd.DataFrame = _fetch_median_family_income(start_date=start_date, end_date=end_date)

        return JSONResponse(content=sanitize_for_json(df))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/mspus")
def get_mspus(
    start_date: str | None = Query(None, description="Filter start date (YYYY-MM-DD)"),
    end_date: str | None = Query(None, description="Filter end date (YYYY-MM-DD)")
):
    """
    Median Sales Price of Houses Sold for the United States (MSPUS)
    """
    try: 
        df:pd.DataFrame = _fetch_median_home_prices(start_date=start_date, end_date=end_date)

        return JSONResponse(content=sanitize_for_json(df))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/mspnus")
def get_msp_new_homes(
    start_date: str | None = Query(None, description="Filter start date (YYYY-MM-DD)"),
    end_date: str | None = Query(None, description="Filter end date (YYYY-MM-DD)")
):
    """
    Median Sales Price for New Houses Sold in the United States (MSPNHSUS)
    """
    try: 
        df:pd.DataFrame = _fetch_median_home_prices(start_date=start_date, end_date=end_date)

        return JSONResponse(content=sanitize_for_json(df))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/cshi")
def get_caseshiller_homes_index(
    start_date: str | None = Query(None, description="Filter start date (YYYY-MM-DD)"),
    end_date: str | None = Query(None, description="Filter end date (YYYY-MM-DD)"),
):
    """S&P CoreLogic Case-Shiller U.S. National Home Price Index (CSUSHPINSA)"""
    try: 
        df:pd.DataFrame = _fetch_caseshiller_home_price_index(start_date=start_date, end_date=end_date) 

        return JSONResponse(content=sanitize_for_json(df))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/hh-ops")
def get_household_ops(
    start_date: str | None = Query(None, description="Filter start date (YYYY-MM-DD)"),
    end_date: str | None = Query(None, description="Filter end date (YYYY-MM-DD)"),
):
    """Expenditures: Household Operations: All Consumer Units (CXUHHOPERLB0101M)"""
    try: 
        df:pd.DataFrame = _fetch_houshold_ops_spend(start_date=start_date, end_date=end_date) 

        return JSONResponse(content=sanitize_for_json(df))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/used-cars")
def get_used_car_prices(
    start_date: str | None = Query(None, description="Filter start date (YYYY-MM-DD)"),
    end_date: str | None = Query(None, description="Filter end date (YYYY-MM-DD)"),
):
    """CPI Used Cars and Trucks (CUSR0000SETA02). Prices calculated based on CPI index applied to reference year and price"""
    try: 
        df:pd.DataFrame = _fetch_used_car_prices(start_date=start_date, end_date=end_date) 

        return JSONResponse(content=sanitize_for_json(df))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/new-cars")
def get_new_car_prices(
    start_date: str | None = Query(None, description="Filter start date (YYYY-MM-DD)"),
    end_date: str | None = Query(None, description="Filter end date (YYYY-MM-DD)"),
):
    """CPI New Cars and Trucks (CUUR0000SETA01). Prices calculated based on CPI index applied to reference year and price"""
    try: 
        df:pd.DataFrame = _fetch_new_car_prices(start_date=start_date, end_date=end_date) 

        return JSONResponse(content=sanitize_for_json(df))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/vehicle-insurance")
def get_vehicle_ins_premiums(
    start_date: str | None = Query(None, description="Filter start date (YYYY-MM-DD)"),
    end_date: str | None = Query(None, description="Filter end date (YYYY-MM-DD)"),
):
    """Expenditures: Vehicle Insurance: All Consumer Units (CXU500110LB0101M)"""
    try: 
        df:pd.DataFrame = _fetch_vehicle_ins_premiums(start_date=start_date, end_date=end_date) 

        return JSONResponse(content=sanitize_for_json(df))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/pce-healthcare")
def get_pce_healthcare(
    start_date: str | None = Query(None, description="Filter start date (YYYY-MM-DD)"),
    end_date: str | None = Query(None, description="Filter end date (YYYY-MM-DD)"),
):
    """PCE Services: Healthcare (DHLCRC1Q027SBEA)."""
    try: 
        df:pd.DataFrame = _fetch_pce_healthcare(start_date=start_date, end_date=end_date) 

        return JSONResponse(content=sanitize_for_json(df))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/unrate")
def get_unrate(
    start_date: str | None = Query(None, description="Filter start date (YYYY-MM-DD)"),
    end_date: str | None = Query(None, description="Filter end date (YYYY-MM-DD)"),
    freq: str = Query(None, description="Frequency period")
):
    """Unemployment Rate (UNRATE)"""
    try: 
        df:pd.DataFrame = _fetch_unrate(start_date=start_date, end_date=end_date, freq=freq)   

        return JSONResponse(content=sanitize_for_json(df))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/m2-supply")
def get_m2_supply(
    start_date: str | None = Query(None, description="Filter start date (YYYY-MM-DD)"),
    end_date: str | None = Query(None, description="Filter end date (YYYY-MM-DD)"),
    freq: str = Query(None, description="Frequency period")
):
    """M2 (WM2NS)"""
    try: 
        df:pd.DataFrame = _fetch_m2_supply(start_date=start_date, end_date=end_date, freq=freq)   

        return JSONResponse(content=sanitize_for_json(df))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/m2-velocity")
def get_m2_velocity(
    start_date: str | None = Query(None, description="Filter start date (YYYY-MM-DD)"),
    end_date: str | None = Query(None, description="Filter end date (YYYY-MM-DD)"),
    freq: str = Query(None, description="Frequency period")
):
    """Velocity of M2 Money Stock (M2V)"""
    try: 
        df:pd.DataFrame = _fetch_m2_velocity(start_date=start_date, end_date=end_date, freq=freq)   

        return JSONResponse(content=sanitize_for_json(df))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/gdp")
def get_gdp(
    start_date: str | None = Query(None, description="Filter start date (YYYY-MM-DD)"),
    end_date: str | None = Query(None, description="Filter end date (YYYY-MM-DD)"),
    freq: str = Query(None, description="Frequency period")
):
    """Gross Domestic Product (GDP)"""
    try: 
        df:pd.DataFrame = _fetch_gdp(start_date=start_date, end_date=end_date, freq=freq)   

        return JSONResponse(content=sanitize_for_json(df))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/sofr")
def get_sofr(
    start_date: str | None = Query(None, description="Filter start date (YYYY-MM-DD)"),
    end_date: str | None = Query(None, description="Filter end date (YYYY-MM-DD)"),
    freq:str = Query(None, description="Frequency Period")
):
    """Secured Overnight Financing Rate (SOFR)"""
    try: 
        df:pd.DataFrame = _fetch_sofr(start_date=start_date, end_date=end_date, freq=freq)   

        return JSONResponse(content=sanitize_for_json(df))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/us-birthrate")
def get_us_birthrate(
    start_date: str | None = Query(None, description="Filter start date (YYYY-MM-DD)"),
    end_date: str | None = Query(None, description="Filter end date (YYYY-MM-DD)"),
    freq:str = Query('A', description="Frequency period")
):
    """Crude Birth Rate for the United States (SPDYNCBRTINUSA)"""
    try: 
        df:pd.DataFrame = _fetch_us_birthrate(start_date=start_date, end_date=end_date, freq=freq)   

        return JSONResponse(content=sanitize_for_json(df))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/home-affordability")
def get_home_affordability(
    start_year: int | None = Query(None, description="Filter start year (YYYY)"),
    end_year: int | None = Query(None, description="Filter end year (YYYY)"),
):
    """
    Merged Report exploring prices and premiums of buying a home over the years.
    """
    try: 
        df:pd.DataFrame = _build_home_affordability(start_year=start_year, end_year=end_year)   

        return JSONResponse(content=sanitize_for_json(df))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/unemployed")
def get_unemployed(
    start_date: str | None = Query(None, description="Filter start date (YYYY-MM-DD)"),
    end_date: str | None = Query(None, description="Filter end date (YYYY-MM-DD)"),
    freq:str = Query('M', description="Frequency period")
):
    """Unemployment Level (UNEMPLOY) as count of Unemployed"""
    try: 
        df:pd.DataFrame = _fetch_unemployment_level(start_date=start_date, end_date=end_date, freq=freq)   

        return JSONResponse(content=sanitize_for_json(df))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/job-openings")
def get_job_openings(
    start_date: str | None = Query(None, description="Filter start date (YYYY-MM-DD)"),
    end_date: str | None = Query(None, description="Filter end date (YYYY-MM-DD)"),
    freq:str = Query('M', description="Frequency period")
):
    """Job Openings: Total Nonfarm (JTSJOL)"""
    try: 
        df:pd.DataFrame = _fetch_job_openings(start_date=start_date, end_date=end_date, freq=freq)   

        return JSONResponse(content=sanitize_for_json(df))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/fed-funds")
def get_fed_funds_rate(
    start_date: str | None = Query(None, description="Filter start date (YYYY-MM-DD)"),
    end_date: str | None = Query(None, description="Filter end date (YYYY-MM-DD)"),
    freq:str = Query('M', description="Frequency period")
):
    """Federal Funds Effective Rate (FEDFUNDS)"""
    try: 
        df:pd.DataFrame = _fetch_fed_funds_rate(start_date=start_date, end_date=end_date, freq=freq)   

        return JSONResponse(content=sanitize_for_json(df))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# New Houses for Sale by Stage of Construction, Not Started (NHFSEPNTS)
@app.get("/new-homes-ns")
def get_new_homes_ns(
    start_date: str | None = Query(None, description="Filter start date (YYYY-MM-DD)"),
    end_date: str | None = Query(None, description="Filter end date (YYYY-MM-DD)"),
    freq:str = Query('M', description="Frequency period")
):
    """New Houses for Sale by Stage of Construction, Not Started (NHFSEPNTS)"""
    try: 
        df:pd.DataFrame = _fetch_new_homes_ns(start_date=start_date, end_date=end_date, freq=freq)   

        return JSONResponse(content=sanitize_for_json(df))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

@app.get("/new-homes-uc")
def get_new_homes_uc(
    start_date: str | None = Query(None, description="Filter start date (YYYY-MM-DD)"),
    end_date: str | None = Query(None, description="Filter end date (YYYY-MM-DD)"),
    freq:str = Query('M', description="Frequency period")
):
    """New Houses for Sale (Units) by Stage of Construction, Under Construction (NHFSEPUCS)"""
    try: 
        df:pd.DataFrame = _fetch_new_homes_uc(start_date=start_date, end_date=end_date, freq=freq)   

        return JSONResponse(content=sanitize_for_json(df))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/new-homes-comp")
def get_new_homes_comp(
    start_date: str | None = Query(None, description="Filter start date (YYYY-MM-DD)"),
    end_date: str | None = Query(None, description="Filter end date (YYYY-MM-DD)"),
    freq:str = Query('M', description="Frequency period")
):
    """New Houses for Sale (Units) by Stage of Construction, Under Construction (NHFSEPUCS)"""
    try: 
        df:pd.DataFrame = _fetch_new_homes_comp(start_date=start_date, end_date=end_date, freq=freq)   

        return JSONResponse(content=sanitize_for_json(df))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/us-births-deaths-by-race")
def get_birth_death_data(
    start_year: int | None = Query(None, description="Filter start year (e.g. 2000)"),
    end_year: int | None = Query(None, description="Filter end year (e.g. 2023)"),
    race: str | None = Query(None, description="Race/Ethnicity filter ('All', 'White', 'Black', 'Hispanic')")
):
    """Births and Deaths by Race/Ethnicity (CDC)"""
    try:
        df: pd.DataFrame = _fetch_birth_death_data(start_year=start_year, end_year=end_year, race=race)
        return JSONResponse(content=df.to_dict(orient="records"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))