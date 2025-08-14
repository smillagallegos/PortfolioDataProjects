import requests 
import pandas as pd 
import os
import subprocess
import time

def download_raw_csv(url: str, folder: str) -> str:
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
    
    max_retries = 5

    for attempt in range(1, max_retries + 1):
        try:
            # Send HTTP GET request to download CSV
            response = requests.get(url, stream=True, timeout=200) 
            response.raise_for_status() # Raises for non-200 status codes
            with open(file_path, "wb") as f:
                # Extract in in chunks since it is a big file
                chunk_size = int(os.getenv("CFIA_CHUNK_SIZE", 8192))
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
            print(f"Raw data saved as: {file_path}")
            return file_path
        except (ChunkedEncodingError, Timeout, ConnectionError) as e:
            print(f"Attempt {attempt}: {type(e).__name__} - {e}. Retrying in 5 seconds...")
            time.sleep(5 * attempt)
        except requests.HTTPError as e:
            raise Exception(f"HTTP error {response.status_code} while downloading file: {e}")

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

    # Load the raw data into a DataFrame
    df = pd.read_csv(input_path) 

    # Check if 'Issue' column exists and filter for key food safety issues
    if 'Issue' in df.columns: 
        filtered_df = df[
                            df['Issue'].str.contains("Salmonella|Listeria|E. Coli", na=False) &
                            ~df['Issue'].str.contains("Listeria - Medical devices", case=False, na=False)
                        ] 

        # Save filtered records to a new CSV
        filtered_df.to_csv(output_path, index=False)

        print(f"Filtered food recalls saved as: {output_path}") 
        print(f"\nFound {len(filtered_df)} food recalls.")
    else:
        # Raise error if expected column is missing
        raise ValueError("'Issue' column not found in dataset.")

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

    # Step 2: Filter for relevant food recall records
    filter_food_recalls(downloaded_file, filtered_path)

# Only run if script is executed directly (not imported)
if __name__ == "__main__":
    main()
