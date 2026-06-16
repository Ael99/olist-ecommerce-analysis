import os
import zipfile
from kaggle.api.kaggle_api_extended import KaggleApi
from loguru import logger
from config import KAGGLE_DATASET, DATA_DIR

def download_dataset():
    os.makedirs(DATA_DIR, exist_ok=True)

    # Skip download if files already exist
    marker = os.path.join(DATA_DIR, "olist_orders_dataset.csv")
    if os.path.exists(marker):
        logger.info("Dataset already downloaded, skipping.")
        return

    logger.info(f"Downloading {KAGGLE_DATASET} from Kaggle...")
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(KAGGLE_DATASET, path=DATA_DIR, unzip=True)
    logger.success(f"Dataset downloaded to {DATA_DIR}")