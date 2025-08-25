import pandas as pd
from pathlib import Path
import sys

# Add private folder to sys.path
sys.path.append(str(Path(__file__).resolve().parent / "cfia_private_utils"))

# Import private DB functions
from db_utils import load_to_database
    
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
    df = pd.read_csv(processed_file_path, skiprows=1)
    if df.empty:
        raise Exception("Processed file is empty after read. Aborting pipeline.")

    # Validate the structure of the DataFrame
    required_columns = ['NID', 'Title', 'URL', 'Product', 'Issue', 
                        'Category', 'Recall class', 'Last updated', 'Archived']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise Exception(f"Processed file is missing required columns: {missing_columns}")

    # Call private DB utility
    load_to_database(df)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Pipeline failed: {e}", file=sys.stderr)
        sys.exit(1)  # Forces non-zero exit code
