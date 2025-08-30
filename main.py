from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
import pandas as pd
from functools import reduce
from utils import sanitize_for_json
import inspect
import categories.demographics as demographics
import categories.housing as housing
import categories.income_and_spending as income_and_spending
import categories.inflation_and_prices as inflation_and_prices
import categories.money_aggregates as money_aggregates
import categories.output_and_growth as output_and_growth
import categories.rates as rates
import categories.wages_and_employment as wages_and_employment


modules = {
    "Demographics": demographics,
    "Housing": housing,
    "Income and Spending": income_and_spending,
    "Inflation and Prices": inflation_and_prices,
    "Money Aggregates": money_aggregates,
    "Output and Growth": output_and_growth,
    "Rates": rates,
    "Wages and Employment": wages_and_employment,
}


app = FastAPI(title="GovData API", version="0.1.0")


@app.get("/")
def root():
    categorized = {}
    for name, module in modules.items():
        categorized[name] = []
        for fn_name, fn in module.__dict__.items():
            if callable(fn) and fn_name.startswith("_fetch"):
                categorized[name].append({
                    "name": fn_name.replace("_fetch_", ""),
                    "description": inspect.getdoc(fn) or ""
                })
    return {"message": "GovData API", "available_datasets": categorized}


@app.get("/cpi")
def get_cpi(
    start_date: str | None = Query(None, description="Filter start date (YYYY-MM-DD)"),
    end_date: str | None = Query(None, description="Filter end date (YYYY-MM-DD)"),
):
    """Consumer Price Index for All Urban Consumers (CPIAUCSL)."""
    try: 
        df:pd.DataFrame = inflation_and_prices._fetch_cpi(start_date=start_date, end_date=end_date)

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
        df:pd.DataFrame = inflation_and_prices._fetch_pce(start_date=start_date, end_date=end_date) 

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
        df:pd.DataFrame = demographics._fetch_us_households(start_date=start_date, end_date=end_date)

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
        df:pd.DataFrame = demographics._fetch_us_population(start_date=start_date, end_date=end_date)

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
        df:pd.DataFrame = income_and_spending._fetch_median_family_income(start_date=start_date, end_date=end_date)

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
        df:pd.DataFrame = rates._fetch_30yr_mortgage_rates(start_date=start_date, end_date=end_date, freq=freq)

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
        df:pd.DataFrame = rates._fetch_15yr_mortgage_rates(start_date=start_date, end_date=end_date, freq=freq)

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
        df:pd.DataFrame = income_and_spending._fetch_median_family_income(start_date=start_date, end_date=end_date)

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
        df:pd.DataFrame = housing._fetch_median_home_prices(start_date=start_date, end_date=end_date)

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
        df:pd.DataFrame = housing._fetch_median_home_prices(start_date=start_date, end_date=end_date)

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
        df:pd.DataFrame = housing._fetch_caseshiller_home_price_index(start_date=start_date, end_date=end_date) 

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
        df:pd.DataFrame = income_and_spending._fetch_houshold_ops_spend(start_date=start_date, end_date=end_date) 

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
        df:pd.DataFrame = inflation_and_prices._fetch_used_car_prices(start_date=start_date, end_date=end_date) 

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
        df:pd.DataFrame = inflation_and_prices._fetch_new_car_prices(start_date=start_date, end_date=end_date) 

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
        df:pd.DataFrame = income_and_spending._fetch_vehicle_ins_premiums(start_date=start_date, end_date=end_date) 

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
        df:pd.DataFrame = income_and_spending._fetch_pce_healthcare(start_date=start_date, end_date=end_date) 

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
        df:pd.DataFrame = wages_and_employment._fetch_unrate(start_date=start_date, end_date=end_date, freq=freq)   

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
        df:pd.DataFrame = money_aggregates._fetch_m2_supply(start_date=start_date, end_date=end_date, freq=freq)   

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
        df:pd.DataFrame = money_aggregates._fetch_m2_velocity(start_date=start_date, end_date=end_date, freq=freq)   

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
        df:pd.DataFrame = output_and_growth._fetch_gdp(start_date=start_date, end_date=end_date, freq=freq)   

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
        df:pd.DataFrame = rates._fetch_sofr(start_date=start_date, end_date=end_date, freq=freq)   

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
        df:pd.DataFrame = demographics._fetch_us_birthrate(start_date=start_date, end_date=end_date, freq=freq)   

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
        df:pd.DataFrame = income_and_spending._fetch_build_home_affordability(start_year=start_year, end_year=end_year)   

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
        df:pd.DataFrame = wages_and_employment._fetch_unemployment_level(start_date=start_date, end_date=end_date, freq=freq)   

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
        df:pd.DataFrame = wages_and_employment._fetch_job_openings(start_date=start_date, end_date=end_date, freq=freq)   

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
        df:pd.DataFrame = rates._fetch_fed_funds_rate(start_date=start_date, end_date=end_date, freq=freq)   

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
        df:pd.DataFrame = housing._fetch_new_homes_ns(start_date=start_date, end_date=end_date, freq=freq)   

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
        df:pd.DataFrame = housing._fetch_new_homes_uc(start_date=start_date, end_date=end_date, freq=freq)   

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
        df:pd.DataFrame = housing._fetch_new_homes_comp(start_date=start_date, end_date=end_date, freq=freq)   

        return JSONResponse(content=sanitize_for_json(df))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/new-sf-homes-for-sale")
def get_new_sf_homes_for_sale(
    start_date: str | None = Query(None, description="Filter start date (YYYY-MM-DD)"),
    end_date: str | None = Query(None, description="Filter end date (YYYY-MM-DD)"),
    freq:str = Query('M', description="Frequency period")
):
    """New One Family Houses for Sale in the United States (HNFSUSNSA)"""
    try: 
        df:pd.DataFrame = housing._fetch_new_sf_homes_for_sale(start_date=start_date, end_date=end_date, freq=freq)   

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
        df: pd.DataFrame = demographics._fetch_birth_death_data(start_year=start_year, end_year=end_year, race=race)
        return JSONResponse(content=df.to_dict(orient="records"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))