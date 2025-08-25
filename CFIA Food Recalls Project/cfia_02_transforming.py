from pathlib import Path
import sys

# Add private folder to sys.path
sys.path.append(str(Path(__file__).resolve().parent / "cfia_private_utils"))

# Import private transformation functions
from transform_utils import (
    load_recall_data, clean_recalls_data,
    process_recalls_columns, parse_issue,
    save_processed_data
)

def main():
    """
    Main function to load and clean yesterday's recall data file.
    """
    # Convert string into Path object
    dir_path = Path("recalls")
    # Get full path to read the file
    recalls_file_path = dir_path / "cfia_food_recalls.csv"
    processed_file_path = dir_path / "processed_cfia_food_recalls.csv"

    # Check if the directory exists
    if not dir_path.exists() or not dir_path.is_dir():
        raise Exception(f"Directory {dir_path.name} does not exist.")

    # Call the function to get recalls data frame
    df_recalls = load_recall_data(recalls_file_path)
    if df_recalls.empty:
        raise Exception(f"Data frame {filename} not found")

    # Call the function to clean the data frame (missing values, duplicates, etc.)
    df_recalls_clean = clean_recalls_data(df_recalls)

    # Call the function to convert to different data type or fill missing values
    df_recalls_processed = process_recalls_columns(df_recalls_clean)

    # Clasify Issues by Subcategories (Second Issue / Bacteria Subtype)
    df_recalls_processed[['Main issue', 'Secondary issue', 'Bacteria subtype']] = df_recalls_processed['Issue'].apply(parse_issue)

    # Show a preview of the data
    print(f"\n{df_recalls_processed.head(10)}")

    # Call the script to save the processed data
    save_processed_data(processed_file_path, df_recalls_processed)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Pipeline failed: {e}", file=sys.stderr)
        sys.exit(1)  # Forces non-zero exit code


