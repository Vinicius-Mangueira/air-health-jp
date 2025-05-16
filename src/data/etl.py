import os
import requests
import pandas as pd
from datetime import datetime

# Municipal air quality API base URL
API_QUALIDADE_AR_URL = "https://api.municipal.gov.br/qualidade_ar"

# DATASUS hospitalizations API base URL
DATASUS_URL = "https://datasus.saude.gov.br/api/internacoes"

# Raw data directory
RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")

def fetch_and_store_air_quality(year_month: str):
    """
    Fetches air quality data for all stations for the specified month
    and saves it as a CSV in data/raw/air_quality_YYYY-MM.csv.

    :param year_month: string in 'YYYY-MM' format
    """
    os.makedirs(RAW_DIR, exist_ok=True)

    stations = [101, 102, 103, 104]  # Customize station IDs as needed
    records = []
    for station_id in stations:
        params = {'station': station_id, 'month': year_month}
        resp = requests.get(API_QUALIDADE_AR_URL, params=params)
        resp.raise_for_status()
        data = resp.json()
        for entry in data.get('readings', []):
            entry.update({'station': station_id})
            records.append(entry)

    df = pd.DataFrame(records)
    filepath = os.path.join(RAW_DIR, f"air_quality_{year_month}.csv")
    df.to_csv(filepath, index=False)
    print(f"Air quality data saved to: {filepath}")


def fetch_and_store_hospitalizations(year_month: str):
    """
    Fetches hospital admission data from DATASUS for the specified month
    and saves it as a CSV in data/raw/hospitalizations_YYYY-MM.csv.

    :param year_month: string in 'YYYY-MM' format
    """
    os.makedirs(RAW_DIR, exist_ok=True)

    params = {'month': year_month}
    resp = requests.get(DATASUS_URL, params=params)
    resp.raise_for_status()
    data = resp.json()

    df = pd.DataFrame(data.get('result', []))
    filepath = os.path.join(RAW_DIR, f"hospitalizations_{year_month}.csv")
    df.to_csv(filepath, index=False)
    print(f"Hospitalization data saved to: {filepath}")


def download_hospitalizations_jp_icd_j00_j99(year_month: str = None):
    """
    Downloads hospital admission data filtered by city 'João Pessoa' and ICD codes J00–J99.
    Saves the result as data/raw/internacoes_jp.csv.

    :param year_month: optional string in 'YYYY-MM' format; defaults to current month
    """
    os.makedirs(RAW_DIR, exist_ok=True)

    if year_month is None:
        year_month = datetime.today().strftime('%Y-%m')

    params = {
        'month': year_month,
        'city': 'João Pessoa',
        'cid_start': 'J00',
        'cid_end': 'J99'
    }
    resp = requests.get(DATASUS_URL, params=params)
    resp.raise_for_status()
    data = resp.json()

    df = pd.DataFrame(data.get('result', []))
    filepath = os.path.join(RAW_DIR, "internacoes_jp.csv")
    df.to_csv(filepath, index=False)
    print(f"Filtered hospitalizations for João Pessoa saved to: {filepath}")


if __name__ == "__main__":
    current_month = datetime.today().strftime('%Y-%m')
    fetch_and_store_air_quality(current_month)
    fetch_and_store_hospitalizations(current_month)
    download_hospitalizations_jp_icd_j00_j99(current_month)
