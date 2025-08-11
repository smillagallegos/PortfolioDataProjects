import pandas as pd
from pathlib import Path
from cfia_02_transforming import get_yesterday_filename
import sqlalchemy 
from sqlalchemy import text
from dotenv import load_dotenv
import sys
import os

# Load variables from .env file
load_dotenv()

def get_sqlalchemy_engine():
    """
    Create and return a SQLAlchemy engine with fast_executemany enabled for SQL Server.
    """

    server = os.getenv("CFIA_SQL_SERVER")
    database = os.getenv("CFIA_SQL_DATABASE")

    if not server or not database:
        raise ValueError("Environment variables CFIA_SQL_SERVER and CFIA_SQL_DATABASE must be set in the .env file.")

    driver = 'ODBC Driver 17 for SQL Server'
    conn_str = (
        f"mssql+pyodbc://@{server}/{database}"
        "?driver=ODBC+Driver+17+for+SQL+Server"
        "&trusted_connection=yes"
    )
    engine = sqlalchemy.create_engine(conn_str, fast_executemany=True)
    return engine

def fetch_existing_ids(engine):
    """
    Fetch all existing NIDs from the FoodRecalls table using SQLAlchemy.
    """
    with engine.connect() as conn:
        result = conn.execute(text("SELECT NID FROM dbo.FoodRecalls"))
        existing_ids = {row[0] for row in result}
    return existing_ids

def main():
    """
        Main function to execute the data insertion pipeline.

        This function is the main entry point for the script. It performs the following tasks:
        1. Reads the data from a CSV file into a Pandas DataFrame.
        2. Connects to the SQL Server database.
        3. Fetches the existing IDs from the database to check for duplicates.
        4. Inserts new records (those not already in the database) into the SQL Server.
        5. Handles exceptions and ensures that any errors are reported.

        If the script encounters an error during execution, it will print the error message 
        and halt the process.
    """
     # Convert string into Path object
    dir_path = Path("recalls")

    # Check if the directory exists
    if not dir_path.exists() or not dir_path.is_dir():
        print(f"Directory {dir_path.name} does not exist.")
        sys.exit(1)

    # Call function to get yesterday's file name
    filename = get_yesterday_filename("processed_cfia_food_recalls_", suffix=".csv")
    processed_file_path = dir_path / filename
    
    # Read the CSV file into a DataFrame
    df = pd.read_csv(processed_file_path)

    # Connect with SQLAlchemy engine
    engine = get_sqlalchemy_engine()

    # Fetch existing IDs to avoid duplicates
    existing_ids = fetch_existing_ids(engine)
    print(f"Found {len(existing_ids)} existing IDs in the database.")

    # Filter out records that already exist
    df_new = df[~df['NID'].isin(existing_ids)]

    if not df_new.empty:
        # Prepare DataFrame for SQL (rename columns if needed)
        column_mapping = {
            "NID": "NID",
            "Title": "Title",
            "URL": "URL",
            "Product": "Product",
            "Issue": "Issue",
            "Main issue": "MainIssue",
            "Secondary issue": "SecondaryIssue",
            "Bacteria subtype": "BacteriaSubtype",
            "Category": "Category",
            "Recall class": "Class",
            "Last updated": "LastUpdated",
            "Archived": "IsArchived"
        }
        # Only use columns present in both DataFrame and mapping
        columns_to_use = [col for col in column_mapping.keys() if col in df_new.columns]
        df_to_insert = df_new[columns_to_use].rename(columns=column_mapping)
        # Insert
        df_to_insert.to_sql(
            "FoodRecalls",
            con=engine,
            if_exists="append",
            index=False,
            method=None  
        )
        print(f"{len(df_to_insert)} records inserted successfully.")
    else:
        print("No new records to insert.")

if __name__ == "__main__":
    main()
