import os
import sys
from pathlib import Path

# Add private folder to sys.path
sys.path.append(str(Path(__file__).resolve().parent / "cfia_private_utils"))

# Import private extraction functions
from extract_utils import download_raw_csv, filter_food_recalls

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
