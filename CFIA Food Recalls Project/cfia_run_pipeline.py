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

    steps = [
        ("cfia_01_extracting.py",  "Extracting"),
        ("cfia_02_transforming.py","Transforming"),
        ("cfia_03_loading.py",     "Loading"),
    ]

    for script, label in steps:
        print(f"\nRunning {label} script: {script} ...")
        try:
            result = subprocess.run(
                ["poetry", "run", "python", script_name],
                 check=True
            )
        except subprocess.CalledProcessError as e:
            print(f"\nPipeline failed during: {label} ({script})")
            if e.stdout:
                print("\n--- STDOUT ---\n" + e.stdout)
            if e.stderr:
                print("\n--- STDERR ---\n" + e.stderr)

            # Exit non-zero on failure
            sys.exit(e.returncode or 1)

    print("\nPipeline completed successfully.")

if __name__ == "__main__":
    run_pipeline()
