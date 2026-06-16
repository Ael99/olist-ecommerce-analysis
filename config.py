import os
from dotenv import load_dotenv

load_dotenv()

# Windows Authentication connection string (no username/password needed)
DB_URL = (
    "mssql+pyodbc://localhost/OlistDB"
    "?driver=ODBC+Driver+17+for+SQL+Server"
    "&Trusted_Connection=yes"
    "&TrustServerCertificate=yes"
)

KAGGLE_DATASET = os.getenv("KAGGLE_DATASET")
DATA_DIR       = os.getenv("DATA_DIR", "./data/raw")
PROCESSED_DIR  = os.getenv("PROCESSED_DIR", "./data/processed")