import requests 
import pandas as pd 
import os
import subprocess
import time
import sys
from requests.exceptions import ChunkedEncodingError, Timeout, ConnectionError
from datetime import datetime
import pytz

def download_raw_csv(url: str, folder: str, max_retries=5, delay=5) -> str:
    """
    Downloads the CFIA raw CSV file from the given URL and saves it to the specified folder.

    Args:
        url (str): The URL to download the CSV from.
        folder (str): The local folder to save the downloaded file.

    Returns:
        str: Path to the saved raw CSV file.
    """
    # Save raw file
    file_name = "cfia_recalls_raw.csv"
    file_path = os.path.join(folder, file_name)

    # Make sure the folder exists
    os.makedirs(folder, exist_ok=True)

    print("Downloading data...")

    for attempt in range(1, max_retries + 1):
        try:
            # Send HTTP GET request to download CSV
            with requests.get(url, stream=True, timeout=200) as response:
                response.raise_for_status() # Raises for non-200 status codes

                # Prepare timestamp header (ET)
                timestamp_et = datetime.now(pytz.timezone("America/Toronto")).strftime("%Y-%m-%d %H:%M:%S %Z")
                header = f"# Last workflow run on (ET): {timestamp_et}\n"

                # Extract in in chunks since it is a big file
                chunk_size = int(os.getenv("CFIA_CHUNK_SIZE", 8192))

                # Open once, write header (text) then streamed chunks (bytes)
                with open(file_path, "wb") as f:
                    f.write(header.encode("utf-8"))
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if chunk:
                            f.write(chunk)
            print(f"Raw data saved as: {file_path}")
            return file_path

        except (ChunkedEncodingError, Timeout, ConnectionError) as e:
            print(f"Attempt {attempt}: {type(e).__name__} - {e}. Retrying in {delay} seconds...")
            if attempt == max_retries:
                raise Exception("Max retries exceeded for downloading raw data.")
            time.sleep(delay * attempt)

        except requests.HTTPError as e:
            raise Exception(f"HTTP error {response.status_code} while downloading file: {e}")
            return None

        except Exception as e:
            print(f"Unexpected error during download: {e}")
            return None

    print("Download failed after maximum retries.")
    return None

def filter_food_recalls(input_path: str, output_path: str) -> int:
    """
    Filters the raw CSV file for food-related recalls and saves it to a new file.

    Args:
        input_path (str): Path to the raw CSV file.
        output_path (str): Path to save the filtered CSV file.

    Returns:
        int: Number of food recall records found.
    """
    print("\nFiltering food recalls...")

    try:
        # Load the raw data into a DataFrame
        df = pd.read_csv(input_path, skiprows=1)
    except Exception as e:
        raise Exception(f"Failed to read input CSV: {e}")

    # Validate the structure of the DataFrame
    required_columns = ['NID', 'Title', 'URL', 'Product', 'Issue', 'Category', 'Recall class', 'Last updated', 'Archived']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise Exception(f"Raw file is missing required columns: {missing_columns}")

    # Check if 'Issue' column exists
    if 'Issue' not in df.columns:
        raise Exception("'Issue' column not found in dataset.")

    try:
        # Filter for food-related bacterial recalls
        filtered_df = df[
            df['Issue'].str.contains("Salmonella|Listeria|E. Coli", na=False) &
            ~df['Issue'].str.contains("Listeria - Medical devices", case=False, na=False)
        ]

        # Generate timestamp in ET
        timestamp_et = datetime.now(pytz.timezone("America/Toronto")).strftime("%Y-%m-%d %H:%M:%S %Z")

        # Save filtered records with timestamp as comment
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(f"# Last workflow run on (ET): {timestamp_et}\n")
            # Save filtered records to a new CSV
            filtered_df.to_csv(output_path, index=False)

        print(f"Filtered food recalls saved as: {output_path}") 
        print(f"\nFound {len(filtered_df)} food recalls.")

        return len(filtered_df)

    except Exception as e:
        raise Exception(f"Error during filtering or saving: {e}")

def main():
    """
    Main function to run the download and filtering steps for CFIA recall data.
    """
    # CFIA open data source for recalls
    url = "https://recalls-rappels.canada.ca/sites/default/files/opendata-donneesouvertes/HCRSAMOpenData.csv"

    # Target folder and dynamic filenames
    folder = "recalls"
    filtered_path = os.path.join(folder, "cfia_food_recalls.csv")

    downloaded_file = download_raw_csv(url, folder)

     # Stop if download failed
    if not downloaded_file or not os.path.exists(downloaded_file):
        raise Exception("Download failed or file is incomplete. Aborting pipeline.")

    # Filter for relevant food recall records
    filtered_df_len = filter_food_recalls(downloaded_file, filtered_path)
    if not filtered_df_len:
        raise Exception("No bacterial food recalls found after filtering. Aborting pipeline.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Pipeline failed: {e}", file=sys.stderr)
        sys.exit(1)  # Forces non-zero exit code
