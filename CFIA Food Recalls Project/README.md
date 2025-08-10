# CFIA Food Recalls Data Pipeline

A Python-based **ETL pipeline** that extracts, cleans, and loads Canadian Food Inspection Agency (CFIA) food recall data into a SQL Server database, enabling analysis of food safety trends in Canada since 2011.

---

## Project Structure

```
cfia-food-recalls/
├── recalls/                      # Processed/raw data files (excluded from repo via .gitignore)
├── cfia_01_extracting.py         # Extracts recall data from CFIA source
├── cfia_02_transforming.py       # Cleans and transforms the data
├── cfia_03_loading.py            # Loads processed data into SQL Server
├── cfia_run_pipeline.py          # Orchestrates the full ETL pipeline
├── pyproject.toml                # Poetry dependency management file
├── README.md                     # Project documentation (you are here)
└── .gitignore                    # Ignore rules for Git
```

---

## Features

- **Automated Data Extraction** – Retrieves recall data from the CFIA's public dataset.
- **Data Cleaning & Transformation** – Standardizes formats, extracts key attributes, and classifies hazards.
- **SQL Server Integration** – Stores processed data in SQL Server using SQLAlchemy for easy querying.
- **Modular Scripts** – Each ETL stage can be run independently or as part of the full pipeline.
- **Automation Ready** – Can be scheduled using Windows Task Scheduler, cron jobs, or adapted for cloud deployment.

---

## Getting Started

### **1. Prerequisites**

- Python **3.12+**
- [Poetry](https://python-poetry.org/) for dependency management
- Microsoft SQL Server (local or cloud)
- (Optional) Task Scheduler, cron, or cloud service for automation

---

### **2. Installation**

```bash
# Clone this repository
git clone https://github.com/smillagallegos/PortfolioDataProjects.git
cd "CFIA Food Recalls Project"

# Install dependencies with Poetry
poetry install
```

---

### **3. Configuration**

Update your database connection details in **`cfia_03_loading.py`**.  
For security, use **environment variables** instead of hardcoding credentials.

Example `.env` file:
```
DB_SERVER=your_server_name
DB_NAME=CFIA_Recalls
DB_USER=your_username
DB_PASSWORD=your_password
```

---

### **4. Running the Pipeline**

Run the full ETL process:
```bash
poetry run python cfia_run_pipeline.py
```

Run individual steps:
```bash
poetry run python cfia_01_extracting.py
poetry run python cfia_02_transforming.py
poetry run python cfia_03_loading.py
```

---

## Data Folder: `recalls/`

Stores the raw and processed CSV files used in the pipeline.  
**Do not upload sensitive or private data** — this folder is ignored by `.gitignore` so it must be created before running the pipeline.

---

## Author

**Salma Milla Gallegos**  
_Data Analyst & Developer_

---

## License

This project is **open-source** and intended for **educational and portfolio purposes** only.
