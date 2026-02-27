import os
import json
import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv
from prefect import task, flow  # Importamos Prefect

load_dotenv()

# --- FASE 1: EXTRACCIÓN Y STAGING ---
@task(retries=3, retry_delay_seconds=60, name="Extraccion y Staging")
def extract_and_stage():
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 10,
        "longitude": -84,
        "hourly": [
            "temperature_2m", "precipitation", "rain", "showers", 
            "relative_humidity_2m", "dew_point_2m", "apparent_temperature", 
            "precipitation_probability", "vapour_pressure_deficit"
        ],
    }
    
    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]
    hourly = response.Hourly()
    
    hourly_data = {
        "date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        ).tolist(),
        "temperature_2m": hourly.Variables(0).ValuesAsNumpy().tolist(),
        "precipitation": hourly.Variables(1).ValuesAsNumpy().tolist(),
        "rain": hourly.Variables(2).ValuesAsNumpy().tolist(),
        "showers": hourly.Variables(3).ValuesAsNumpy().tolist(),
        "relative_humidity_2m": hourly.Variables(4).ValuesAsNumpy().tolist(),
        "dew_point_2m": hourly.Variables(5).ValuesAsNumpy().tolist(),
        "apparent_temperature": hourly.Variables(6).ValuesAsNumpy().tolist(),
        "precipitation_probability": hourly.Variables(7).ValuesAsNumpy().tolist(),
        "vapour_pressure_deficit": hourly.Variables(8).ValuesAsNumpy().tolist()
    }

    if not os.path.exists('staging'): os.makedirs('staging')
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    with open(f"staging/raw_multivariable_{timestamp}.json", 'w') as f:
        json.dump(hourly_data, f, default=str)
    
    return hourly_data

# --- FASE 2: TRANSFORMACIÓN ---
@task(name="Transformacion con Pandas")
def transform(hourly_data):
    df = pd.DataFrame(data=hourly_data)
    df['date'] = pd.to_datetime(df['date'])
    df['fecha_proceso'] = datetime.now()
    
    df.rename(columns={
        'temperature_2m': 'temp_c',
        'precipitation_probability': 'prob_lluvia',
        'relative_humidity_2m': 'humedad'
    }, inplace=True)
    
    return df.tail(24).to_dict(orient='records')

# --- FASE 3: CARGA ---
@task(name="Carga a MongoDB")
def load(data_cleaned):
    client = MongoClient(os.getenv("MONGO_URI"))
    db = client[os.getenv("DB_NAME")]
    collection = db[os.getenv("COLLECTION_NAME")]
    
    result = collection.insert_many(data_cleaned)
    return f"Insertados {len(result.inserted_ids)} registros."

# --- EL FLUJO ORQUESTADO ---
@flow(name="Pipeline Clima ULEAD Final")
def weather_pipeline():
    raw = extract_and_stage()
    clean = transform(raw)
    status = load(clean)
    print(status)

if __name__ == "__main__":
    weather_pipeline()