import pandas as pd
from pathlib import Path
import sqlalchemy 
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from dotenv import load_dotenv
import sys
import os

# Load variables from .env file
load_dotenv()

def get_sqlalchemy_engine(max_retrie=5, delay=5):
    """
    Create and return a SQLAlchemy engine with fast_executemany enabled for SQL Server.
    """

    server = os.getenv("CFIA_SQL_SERVER")       
    database = os.getenv("CFIA_SQL_DATABASE")   
    username = os.getenv("CFIA_SQL_USER")       
    password = os.getenv("CFIA_SQL_PASSWORD")   

    if not all([server, database, username, password]):
        raise ValueError("All required DB environment variables must be set.")

    driver = "ODBC Driver 18 for SQL Server"
    conn_str = (
        f"mssql+pyodbc://{username}:{password}@{server}/{database}"
        f"?driver={driver.replace(' ', '+')}"
        "&Encrypt=yes"
        "&TrustServerCertificate=no"
    )

    # Logic to retry the connection to the db
    for attempt in range(1, max_retries + 1):
        try:
            engine = sqlalchemy.create_engine(conn_str, fast_executemany=True)
            # Test connection
            with engine.connect() as conn:
                pass
            print("Database connection established.")
            return engine

        except OperationalError as e:
            print(f"Attempt {attempt}: Database connection failed - {e}")
            if attempt == max_retries:
                raise Exception("Max retries exceeded for SQL database connection.")
            time.sleep(delay * attempt)


def fetch_existing_ids(engine):
    """
    Fetch all existing NIDs from the FoodRecalls table using SQLAlchemy.
    """
    with engine.connect() as conn:
        result = conn.execute(text("SELECT NID FROM dbo.FoodRecalls"))
        return {row[0] for row in result}
    
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
        raise Exception(f"Directory {dir_path.name} does not exist.")

    # Call function to get processed file
    processed_file_path = dir_path / "processed_cfia_food_recalls.csv"
    
    if not processed_file_path.exists():
        raise Exception(f"File {processed_file_path.name} does not exist.")

    # Read the CSV file into a DataFrame and ignore timestamp comment
    df = pd.read_csv(processed_file_path, comment="#")
    if df.empty:
        raise Exception("Processed file is empty after read. Aborting pipeline.")

    # Validate the structure of the DataFrame
    required_columns = ['NID', 'Title', 'URL', 'Product', 'Issue', 'Category', 'Recall class', 'Last updated']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise Exception(f"Processed file is missing required columns: {missing_columns}")

    # Connect with SQLAlchemy engine
    engine = get_sqlalchemy_engine()

    try:
        # Fetch existing IDs to avoid duplicates
        try:
            existing_ids = fetch_existing_ids(engine)
            print(f"Found {len(existing_ids)} existing IDs in the database.")
        except Exception as e:
            raise Exception(f"Failed to fetch existing IDs: {e}")

        # Filter out records that already exist
        df_new = df[~df['NID'].isin(existing_ids)]

        if df_new.empty:
            print("No new records to insert. Exiting gracefully.")
            return

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
            
        try:
            # Insert
            with engine.begin() as conn:
                df_to_insert.to_sql(
                    "FoodRecalls",
                    con=conn,            
                    schema="dbo",        
                    if_exists="append",
                    index=False,
                    method=None,         
                )

            print(f"{len(df_to_insert)} records inserted successfully.")

        except Exception as e:
            raise Exception(f"Insert failed : {e}")

    finally:
        # ensure pool is torn down in all cases
        engine.dispose()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Pipeline failed: {e}", file=sys.stderr)
        sys.exit(1)  # Forces non-zero exit code