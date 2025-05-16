# src/data/load_data.py
import os
import pandas as pd
from datetime import datetime

# Directories
RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")


def load_air_quality(year_month: str) -> pd.DataFrame:
    """
    Load air quality CSV for given month from raw directory.
    """
    path = os.path.join(RAW_DIR, f"air_quality_{year_month}.csv")
    df = pd.read_csv(path)
    return df


def load_hospitalizations(year_month: str) -> pd.DataFrame:
    """
    Load all hospitalizations CSV for given month.
    """
    path = os.path.join(RAW_DIR, f"hospitalizations_{year_month}.csv")
    df = pd.read_csv(path)
    return df


def load_hospitalizations_jp() -> pd.DataFrame:
    """
    Load João Pessoa filtered hospitalizations.
    """
    path = os.path.join(RAW_DIR, "internacoes_jp.csv")
    df = pd.read_csv(path)
    return df


# --------------------------------------------------------------
# src/data/etl.py (append or integrate cleaning & aggregation)
import pandas as pd


def clean_data(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    """
    Convert date column to datetime and handle missing values.
    Numeric columns: fill NA with column mean.
    Others: drop rows with NA.
    """
    # Convert date
    df[date_col] = pd.to_datetime(df[date_col])

    # Separate numeric vs non-numeric
    num_cols = df.select_dtypes(include="number").columns
    obj_cols = df.select_dtypes(exclude="number").columns.drop(date_col, errors='ignore')

    # Fill numeric NAs
    for col in num_cols:
        mean = df[col].mean()
        df[col] = df[col].fillna(mean)

    # Drop remaining NAs in non-numeric
    df = df.dropna(subset=obj_cols)
    return df


def aggregate_monthly(air_df: pd.DataFrame, hosp_df: pd.DataFrame, jp_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate by month: pollutant means and hospitalization sums.
    Returns merged monthly DataFrame.
    """
    # Set date index
    air_df = air_df.set_index('timestamp')
    hosp_df = hosp_df.set_index('admission_date')
    jp_df = jp_df.set_index('admission_date')

    # Resample
    air_m = air_df.resample('M').mean().add_prefix('air_')
    hosp_m = hosp_df.resample('M').size().to_frame('hospitalizations_total')
    jp_m = jp_df.resample('M').size().to_frame('hospitalizations_jp')

    # Merge all
    merged = air_m.join(hosp_m, how='outer').join(jp_m, how='outer')
    merged = merged.fillna(0)
    return merged


def save_monthly(merged_df: pd.DataFrame, output_path: str):
    """
    Save merged monthly data to CSV.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    merged_df.to_csv(output_path)
    print(f"Monthly merged data saved to: {output_path}")


# Example main for 4.3
if __name__ == "__main__":
    month = datetime.today().strftime('%Y-%m')
    air = load_air_quality(month)
    hosp = load_hospitalizations(month)
    jp = load_hospitalizations_jp()

    air_clean = clean_data(air, 'timestamp')
    hosp_clean = clean_data(hosp, 'admission_date')
    jp_clean = clean_data(jp, 'admission_date')

    merged = aggregate_monthly(air_clean, hosp_clean, jp_clean)
    out_path = os.path.join(os.path.dirname(__file__), "..", "data", "processed", "merged_monthly.csv")
    save_monthly(merged, out_path)