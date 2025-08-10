import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import subprocess
import traceback
import numpy as np
import re

def get_yesterday_filename(prefix="cfia_food_recalls_", suffix=".csv") -> str:
    """
        Generate yesterday's filename (since the file is updated at 2:00 AM for today's date) using the standard naming convention.

        Args:
            prefix (str): The filename prefix (default is 'cfia_food_recalls_').
            suffix (str): The filename extension (default is '.csv').
        
        Returns:
            str: The generated filename in the format 'prefixYYYY_MM_DD.csv'.
    """
    # Get yesterday's date string
    yesterday = datetime.now() - timedelta(days=1)
    yesterday_date_str = yesterday.strftime("%Y_%m_%d")
    return f"{prefix}{yesterday_date_str}{suffix}"

def load_recall_data(recalls_file_path: Path) -> pd.DataFrame:
    """
    Load the recall data from the given file path.

    Args:
        recalls_file_path (Path): The full path to the recall CSV file.

    Returns:
        pd.DataFrame: A DataFrame with the loaded data, or empty if file not found.
    """
    # Check if the file exists and read it
    if recalls_file_path.exists():
        print(f"Successfully read {recalls_file_path.name}")
        return pd.read_csv(recalls_file_path)
    else:
        print(f"File {recalls_file_path.name} does not exist.")
        return pd.DataFrame() # Create an empty data frame to avoid an error

def clean_recalls_data(df_recalls: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the recall data by removing duplicates and rows with missing values.

    Args:
        df_recalls (pd.DataFrame): Raw DataFrame to be cleaned.

    Returns:
        pd.DataFrame: Cleaned DataFrame with no duplicates or NA values.
    """
    # Check initial shape
    initial_shape = df_recalls.shape

    # Get all duplicated values in the data frame
    duplicates = df_recalls[df_recalls.duplicated(keep=False)]
    print(f"\nTotal duplicated values: {duplicates.shape}")

    # Drop duplicates 
    df_recalls_clean = df_recalls.drop_duplicates()

    # Get all NA values in the data frame
    total_nas = df_recalls_clean.isna().sum()
    print(f"\nTotal missing values:\n{total_nas}")

    # Drop null values in 'Recall class' column
    df_recalls_clean = df_recalls_clean.dropna(subset=["Recall class"])

    # Drop columns where 'Recall class' column contains '--' since this is a key feature for future analysis
    df_recalls_clean = df_recalls_clean[df_recalls_clean['Recall class'] != '--']

    print(f"\nCleaned data: {initial_shape} → {df_recalls_clean.shape}")
    return df_recalls_clean

def extract_product_name(title):
    """
    Extracts the product name from the given title.
    
    This function attempts to capture the product name by using regular expressions.

    Args:
        title (str): The title of the recall item that contains the product name.

    Returns:
        str: The extracted product name, or None if no product name could be found.
    """
    match = re.search(r"\bin (.*)", title, re.IGNORECASE)
    if match:
        return match.group(1).strip()
        
    triggers = [
        r"recalled due to",
        r"recalled",
        r"may contain",
        r"may be contaminated with",
        r"due to",
        r"possible contamination with",
        r"possible presence of",
        r"may be unsafe"
    ]
    pattern = r"^(.*?)\s*(?=(" + "|".join(triggers) + r"))"
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return None

def process_recalls_columns(df_recalls: pd.DataFrame) -> pd.DataFrame:
    """
    Convert data columns to a different data type and fill NA values.

    Args:
        df_recalls (pd.DataFrame): Raw DataFrame to be processed.

    Returns:
        pd.DataFrame: Updated DataFrame with modified columns.
    """
    # 'Recall class' contains 'Class 1 - Class 2' values that will be split into 2 different rows
    df_recalls['Recall class'] = df_recalls['Recall class'].str.split(' - ')

    # Explode the list into separate rows
    df_recalls = df_recalls.explode('Recall class', ignore_index=True)

    # Map 'Recall class' column values to numbers and convert this column to numeric type
    df_recalls['Recall class'] = df_recalls['Recall class'].replace('Type II', 'Class 2').astype(str)

    # Replace "E. Coli O157:H7" with "E. Coli - O157:H7" to standardize it for parse_issue function
    df_recalls['Issue'] = df_recalls['Issue'].replace('E. Coli O157:H7', 'E. Coli - O157:H7')

    # Convert 'Archived' column to numeric type
    df_recalls['Archived'] = df_recalls['Archived'].astype(int)

    # Apply the function only to rows where 'Product' is NaN
    # This will ensure that only the missing product names are populated
    df_recalls.loc[df_recalls['Product'].isna(), 'Product'] = df_recalls.loc[df_recalls['Product'].isna(), 'Title'].apply(extract_product_name)

    # Display the updated DataFrame with titles and their corresponding product names
    print(df_recalls[['Title', 'Product']])

    return df_recalls

def parse_issue(issue):
    """
    Parses the recall issue string and extracts the main issue, any secondary issue or hazard, 
    and the subtype (if applicable).

    The function splits the issue string by ' - ' and applies special logic for certain cases:
        - If the main issue is 'Listeria' and the secondary part is 'Food', the record is treated as 'Listeria' only, with no secondary issue.
        - For 'E. Coli', the first part after the dash is treated as the subtype, and if a third part exists, it is treated as a secondary issue/hazard.
        - For all other cases with a dash, the second part is treated as the secondary issue or hazard.

    Args:
        issue (str): The issue description from the recall data.

    Returns:
        pd.Series: A Pandas Series with three elements:
            - main_issue (str): The primary bacteria or hazard detected (e.g., 'Salmonella', 'Listeria', 'E. Coli').
            - secondary_issue (str): Any additional hazard or bacteria present (blank if none or for 'Listeria - Food').
            - subtype (str): The subtype/serotype for 'E. Coli' if present, otherwise blank.
    """
    # Split by dash, remove leading/trailing whitespace
    parts = [part.strip() for part in issue.split(' - ')]
    main_issue = parts[0]
    subtype = ''
    secondary_issue = ''

    if len(parts) > 1:
        # If a second part exists and contains the word "Food", skip it
        if main_issue.lower() == 'listeria' and parts[1].lower() == 'food':
            pass
        elif main_issue.lower().startswith('e. coli'):
            # If a second part exists and the main issue is "E.Coli" it's a bacteria subtype
            subtype = parts[1]
            # If a third part exists and the main issue is "E.Coli", it's a second issue
            if len(parts) > 2:
                secondary_issue = parts[2]
        else:
            secondary_issue = parts[1]
        
    return pd.Series([main_issue, secondary_issue, subtype])

def save_processed_data (processed_file_path: Path, df_recalls_processed: pd.DataFrame):
    """
    Save the processed recall data to a CSV file.

    This function takes a DataFrame containing processed recall data and saves it 
    to a specified file path. The file is saved without including the index.

    Args:
        processed_file_path (Path): The full path where the processed data should be saved.
        df_recalls_processed (pd.DataFrame): The DataFrame containing the processed recall data.

    Returns:
        None: This function saves the file to the disk and prints a success message.
    """
    # Save updated data to a new CSV
    df_recalls_processed.to_csv(processed_file_path, index=False)

    print(f"\nData successfully saved to {processed_file_path.name}")

def main():
    """
    Main function to load and clean yesterday's recall data file.
    """
    # Convert string into Path object
    dir_path = Path("recalls")

    # Get yesterday's date string
    yesterday = datetime.now() - timedelta(days=1)
    yesterday_date_str = yesterday.strftime("%Y_%m_%d")

    try:
        # Check if the directory exists
        if not dir_path.exists() or not dir_path.is_dir():
            print(f"Directory {dir_path.name} does not exist.")
            return

        # Call function to get yesterday's file name
        filename = get_yesterday_filename()
        recalls_file_path = dir_path / filename
        processed_file_path = dir_path / f"processed_{filename}"

        # Call the function to get recalls data frame
        df_recalls = load_recall_data(recalls_file_path)

        if df_recalls.empty:
            print(f"Data frame {filename} not found")
            return

        # Call the function to clean the data frame (missing values, duplicates, etc.)
        df_recalls_clean = clean_recalls_data(df_recalls)

        # Call the function to make 'Recall class' column numerical
        df_recalls_processed = process_recalls_columns(df_recalls_clean)

        # Clasify Issues by Subcategories (Second Issue / Bacteria Subtype)
        df_recalls_processed[['Main issue', 'Secondary issue', 'Bacteria subtype']] = df_recalls_processed['Issue'].apply(parse_issue)

        # Show a preview of the data
        print(f"\n{df_recalls_processed.head(10)}")

        # Call the script to save the processed data
        save_processed_data(processed_file_path, df_recalls_processed)

    except Exception as e:
        # Catch and print any errors during the pipeline run
        print(f"\nPipeline failed: {e}")
        traceback.print_exc()  # This will print the full traceback, showing the exact line where the error occurred.

if __name__ == "__main__":
    main()


