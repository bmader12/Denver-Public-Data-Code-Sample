"""Helpful tools for both notebooks"""
import pandas as pd
import requests, zipfile
from urllib.request import urlopen
from io import BytesIO
from pathlib import Path
import numpy as np
import seaborn as sns


def read_remote_csv(zip_file_url: str, file_name: str) -> pd.DataFrame:
    remote_zip_file = urlopen(zip_file_url)
    zip_mem = BytesIO(remote_zip_file.read())
    zip_file = zipfile.ZipFile(zip_mem)
    print("MASSIVE CHANGE HERE")
    return pd.read_csv(zip_file.open(file_name), encoding="ISO-8859-1", dtype=str)


def clean_accidents_df(accidents_df: pd.DataFrame) -> pd.DataFrame:
    """
    Check for duplicates
    Using OFFENSE_ID as unique key
    """
    accidents_df[
        accidents_df.duplicated(subset=["OFFENSE_ID"], keep=False)
    ].sort_values(by=["REPORTED_DATE"], ascending=False)

    # Remove Duplicates
    accidents_df.drop_duplicates(
        subset=["INCIDENT_ID", "REPORTED_DATE", "OFFENSE_ID"], inplace=True
    )

    # Convert to date time
    accidents_df["REPORTED_DATE"] = pd.to_datetime(accidents_df["REPORTED_DATE"])
    accidents_df["FIRST_OCCURRENCE_DATE"] = pd.to_datetime(
        accidents_df["FIRST_OCCURRENCE_DATE"]
    )

    # ID Date data quality - first 4 digits of incident ID appear to be year code
    accidents_df["IncidentYear"] = accidents_df["INCIDENT_ID"].str[:4]
    accidents_df["REPORTED_DATE_YEAR"] = accidents_df["REPORTED_DATE"].map(
        lambda x: str(x.year)
    )
    accidents_df["Date Quality Issue"] = (
        accidents_df["REPORTED_DATE_YEAR"] != accidents_df["IncidentYear"]
    )

    # Add date features
    accidents_df["Day_of_week_Reported"] = accidents_df["REPORTED_DATE"].dt.day_name()
    accidents_df["Case_Created_Month"] = accidents_df["REPORTED_DATE"].dt.month
    accidents_df["Case_Created_Hour"] = accidents_df["REPORTED_DATE"].map(
        lambda x: (x.hour)
    )
    return accidents_df


def calc_distance(x: pd.DataFrame) -> float:
    Denver_coordinates = (39.7392, -104.9903)
    row_coordinated = (x["Latitude"], x["Longitude"])
    return geopy.distance.distance(Denver_coordinates, row_coordinated).miles


def clean_service_request_df(service_request_df: pd.DataFrame) -> pd.DataFrame:
    # Calculate how far from denver each set of coordinates is
    service_request_df["Coordinates distance"] = service_request_df[
        ["Latitude", "Longitude"]
    ].apply(
        lambda x: calc_distance(x)
        if not pd.isnull(x["Latitude"]) and not pd.isnull(x["Longitude"])
        else None,
        axis=1,
    )

    # Keep only cooridnates with 100 miles of Denver
    # Other values are presumbed to typos/malfuction and will be discarded
    # More functional knowledge of coordinates are collected (automated vs manual input) to adjust coordinates
    service_request_df["Coordinates cleaned"] = service_request_df[
        ["Latitude", "Longitude", "Coordinates distance"]
    ].apply(
        lambda x: [x["Latitude"], x["Longitude"]]
        if not pd.isnull(x["Coordinates distance"]) and x["Coordinates distance"] < 100
        else None,
        axis=1,
    )

    service_request_df["Case Created dttm"] = pd.to_datetime(
        service_request_df["Case Created dttm"]
    )
    service_request_df["Case Closed dttm"] = pd.to_datetime(
        service_request_df["Case Closed dttm"]
    )
    # Calculated Time to Resolve Request
    service_request_df["Time_To_Resolve_Requests_hour"] = (
        service_request_df["Case Closed dttm"] - service_request_df["Case Created dttm"]
    ).dt.total_seconds() / 3600
    # dttm always provides more information so we can drop the case create and case closed
    service_request_df.drop(
        ["Case Created Date", "Case Closed Date"], axis=1, inplace=True
    )
    # Clean up and combine Zip code columns into one column. Users seem to fill out Zip code in one of these two columns
    service_request_df["Zip_Code_Combined"] = service_request_df[
        ["Customer Zip Code", "Incident Zip Code"]
    ].apply(
        lambda x: x["Incident Zip Code"]
        if not pd.isnull(x["Incident Zip Code"]) and x["Incident Zip Code"].isnumeric()
        else x["Customer Zip Code"],
        axis=1,
    )

    # Convert to 5 digit zip codes - remove trailing 4 digits
    service_request_df["Zip_Code_Combined"] = service_request_df[
        "Zip_Code_Combined"
    ].map(lambda x: x[:5] if type(x) == str else str(x)[:5])
    # Remove non numeric values and values that are not 5 digits
    service_request_df["Zip_Code_Combined"] = service_request_df[
        "Zip_Code_Combined"
    ].map(lambda x: x if len(x) == 5 and x.isnumeric() else None)
    # Select on zips that start with 80, or 81 as all Co Zip codes start with those values
    service_request_df["Zip_Code_Combined"] = service_request_df[
        "Zip_Code_Combined"
    ].map(
        lambda x: x
        if x is not None and (x.startswith("80") or x.startswith("81"))
        else None
    )
    return service_request_df


def traffic_accidents_filter_new_records(df: pd.DataFrame, engine) -> pd.DataFrame:
    # pull in all the existing unique keys
    # As OFFENSE_ID is a unique key groupby and distinct are not needed
    # Pull in OFFENSE_ID and LAST_OCCURRENCE_DATE
    # These fields are selected as a new OFFENSE_ID will be inserted and a different LAST_OCCURRENCE_DATE will result in an update
    traffic_unique_key_result = engine.execute(
        """SELECT CASE WHEN LAST_OCCURRENCE_DATE IS NOT NULL  THEN OFFENSE_ID || 
    LAST_OCCURRENCE_DATE ELSE OFFENSE_ID end as Unique_Key
    FROM traffic_accidents """
    ).fetchall()

    df["LAST_OCCURRENCE_DATE"] = pd.to_datetime(df["LAST_OCCURRENCE_DATE"])

    # extract list of unique keys into a list
    traffic_existing_unique_key = [row[0] for row in traffic_unique_key_result]

    # Create Unique Key by combining OFFENSE_ID and LAST_OCCURRENCE_DATE
    df["unique_key"] = df["OFFENSE_ID"].astype(str) + df[
        "LAST_OCCURRENCE_DATE"
    ].dt.strftime("%Y-%m-%d %H:%M:%S.%f").fillna("")
    return df[~df["unique_key"].isin(traffic_existing_unique_key)]


def write_df_to_sqlite(df: pd.DataFrame, table_name: str, engine) -> int:
    df.to_sql(table_name, con=engine, if_exists="append", index=False)
    return engine.execute(f"SELECT count(*) FROM {table_name}").fetchall()[0]
