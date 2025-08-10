"""
cfia_run_pipeline.py

Orchestrates the full ETL (Extract, Transform, Load) pipeline for CFIA food recall data.

This script sequentially:
    1. Downloads and filters the latest CFIA recall data.
    2. Cleans and processes the filtered data.
    3. Uploads the processed data to a SQL database.

Usage:
    poetry run python run_cfia_pipeline.py

Each step is executed as a separate script using subprocess for modularity and maintainability.

Author: Salma Milla Gallegos
Date: 11/06/2025
"""

import subprocess

def run_pipeline():
    """
    Runs the CFIA food recalls ETL pipeline by executing each processing script in sequence:
        1. Downloads and filters the data.
        2. Cleans the filtered data.
        3. Uploads the cleaned data to the SQL database.

    If any step fails, the process will stop and print an error message.
    """
    print("Starting CFIA data pipeline...")

    try:
        # Step 1: Extract and filter the data
        script_name = "cfia_01_extracting.py"
        print(f"\nRunning extracting script: ...")
        subprocess.run(["poetry", "run", "python", script_name], check=True)
        
        # Step 2: Transform the filtered data
        script_name = "cfia_02_transforming.py"
        print(f"\nRunning transforming script: {script_name}...")
        subprocess.run(["poetry", "run", "python", script_name], check=True)
        
        # Step 3: Load the cleaned data to SQL
        script_name = "cfia_03_loading.py"
        print(f"\nRunning loading script: {script_name}...")
        subprocess.run(["poetry", "run", "python", script_name], check=True)

        print("\nPipeline completed successfully.")

    except subprocess.CalledProcessError as e:
        print(f"\nPipeline failed: {e}")

if __name__ == "__main__":
    run_pipeline()
