from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import pandas as pd
from datasets import (  # <-- import your functions from the module you just wrote
    get_population,
    get_cpi,
    get_pce,
    get_houshold_ops_spend,
    get_vehicle_ins,
    get_total_households,
    get_pce_healthcare,
    get_cpi_prices_used_cars,
    get_cpi_prices_new_cars,
    get_real_disposable_personal_income,
    get_median_home_prices,
    get_median_family_income,
    get_30yr_mortgage_rates,
)

app = FastAPI(title="FRED Data API", version="0.1.0")

# Registry of datasets
datasets = {
    "population": get_population,
    "cpi": get_cpi,
    "pce": get_pce,
    "household_ops": get_houshold_ops_spend,
    "vehicle_ins": get_vehicle_ins,
    "households": get_total_households,
    "pce_healthcare": get_pce_healthcare,
    "used_car_prices": get_cpi_prices_used_cars,
    "new_car_prices": get_cpi_prices_new_cars,
    "rdpi": get_real_disposable_personal_income,
    "home_prices": get_median_home_prices,
    "family_income": get_median_family_income,
    "mortgage_rates": get_30yr_mortgage_rates,
}


@app.get("/")
def root():
    return {"message": "Welcome to the FRED Data API", "available_datasets": list(datasets.keys())}


@app.get("/data/{dataset_name}")
def get_dataset(dataset_name: str):
    if dataset_name not in datasets:
        raise HTTPException(status_code=404, detail="Dataset not found")

    try:
        df: pd.DataFrame = datasets[dataset_name]()
        # Convert DataFrame to JSON
        return JSONResponse(content=df.to_dict(orient="records"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

