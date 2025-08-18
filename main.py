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
    "cshi": _fetch_caseshiller_home_price_index,
    "hh-ops": _fetch_houshold_ops_spend,
    "used-cars": _fetch_used_car_prices,
    "new-cars": _fetch_new_car_prices,
    "vehicle-insurance": _fetch_vehicle_ins_premiums,
    "pce-healthcare": _fetch_pce_healthcare,
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
    end_date: str | None = Query(None, description="Filter end date (YYYY-MM-DD)")
):
    """
    30-Year Fixed Rate Mortgage Average in the United States (MORTGAGE30US)
    """
    try: 
        df:pd.DataFrame = _fetch_30yr_mortgage_rates(start_date=start_date, end_date=end_date)

        return JSONResponse(content=sanitize_for_json(df))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/mortgage-15yr")
def get_15yr_mortgage_rates(
    start_date: str | None = Query(None, description="Filter start date (YYYY-MM-DD)"),
    end_date: str | None = Query(None, description="Filter end date (YYYY-MM-DD)")
):
    """
    15-Year Fixed Rate Mortgage Average in the United States (MORTGAGE15US)
    """
    try: 
        df:pd.DataFrame = _fetch_15yr_mortgage_rates(start_date=start_date, end_date=end_date)

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


@app.get("/cshi")
def get_caseshiller(
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
def get_used_car_prices(
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