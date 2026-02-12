# main.py
import asyncio
from asyncio import tasks
from pathlib import Path
import pandas as pd
from loguru import logger
import os
from dataclasses import dataclass
from validation import BundleValidator
from store_validator.apple_store import AppleStoreConfig
from store_validator.amazon_store import AmazonStoreConfig
from store_validator.microsft_store import MicrosoftManager
from store_validator.gallaxy import GallaxyManager
from store_validator.roku import rokuManager
from store_validator.lgstore import lgstoreManager
from store_validator.Samsung_app_store import Samsung_app_store_Manager
from store_validator.zeasn import ZeasnManager
from store_validator.android_store import appstoreManager
from store_validator.vizio import VizioManager
import sys
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
import pyarrow.parquet as pq
from cache import load_cache
from asyncio import Semaphore
# from Final_data_all_apps import append_data


# Load environment variables
load_dotenv()
SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT")
GOOGLE_SHEET_ID = os.getenv("SHEET_ID")
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME")

# Mapping store-specific developer columns to a standard column
STORE_DEVELOPER_COLS = {
    "apple": ["sellerUrl"],
    "android": ["appstore_developer_url"],
    "amazon": ["appstore_developer_url", "privacyPolicyUrl"],
    "microsoft": ["appstore_developer_url"],
    "gallaxy": ["site"],
    "samsung": ["appstore_developer_url"],
    "zeasn": ["developappstore_developer_urlerSite"],
    "vizio": ["data_developer_url"],
    "roku": ["appstore_developer_url"],
    "lg": ["developer_url"]
}

@dataclass
class DirectoryConfig:
    logs_dir: Path = Path("logs")
    store_logs_dir: Path = Path("store_logs")
    output_dir: Path = Path("output")
    routed_dir: Path = Path("routed_ids")
    semaphore_limit: int = 5

    # # Permanent merged file (kept forever)
    # permanent_merged_file: Path = Path("output/combined_permanent.parquet")
    # # Temporary merged file (can be overwritten/deleted each run)
    # temp_merged_file: Path = Path("output/combined_temp.parquet")

    def __post_init__(self):
        self.logs_dir.mkdir(exist_ok=True)
        self.store_logs_dir.mkdir(exist_ok=True)
        self.output_dir.mkdir(exist_ok=True)
        self.routed_dir.mkdir(exist_ok=True)
        self.semaphore = Semaphore(self.semaphore_limit)
        self.file_name= "Mappint_file_testing.parquet"

    def setup_logger(self):
        logger.remove()
        self.logs_dir.mkdir(exist_ok=True)
        self.log_file = self.logs_dir / "main.log"
        logger.add(
            self.log_file,
            retention="10 days",
            level="INFO",
            format="{time} {level} {file.name}:{function}:{line} - {message}",
            backtrace=True,
            diagnose=True,
            enqueue=True,
        )
        logger.add(sys.stdout, level="INFO")
        logger.info(f"Logging started in file: {self.log_file}")


    def read_google_sheet(self, sheet_name: str, service_account_file: str, sheet_id: str) -> pd.DataFrame:
        scope = ["https://spreadsheets.google.com/feeds",
                 "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(service_account_file, scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(sheet_id).worksheet(sheet_name)
        data = sheet.get_all_records()
        df = pd.DataFrame(data)
        logger.info(f"Loaded {len(df)} rows from Google Sheet '{sheet_name}'")
        return df
    
    def parquet_has_rows(self, path: Path) -> bool:
        try:
            return pq.ParquetFile(path).metadata.num_rows > 0
        except Exception:
            return False


    async def process_store(self, store_name, processor, data):
        async with self.semaphore:
            try:
                # ids = (df.loc[df["store"] == store_name, "bundle_id"].dropna().unique().tolist())
                ids = [i['bundle_id'] for i in data if i['store'] == store_name]

                if not ids:
                    logger.info(f"No bundle IDs for {store_name}")
                    return

                logger.info(f"Processing...... {store_name} with {len(ids)} IDs")
                res = await processor.process(ids)

                return 

            except Exception as e:
                logger.error(f"Error processing {store_name}: {e}")
    


    async def main(self):
        # df_input = self.read_google_sheet(GOOGLE_SHEET_NAME, SERVICE_ACCOUNT_FILE, GOOGLE_SHEET_ID)
        df = pd.read_excel(r"C:\DataBeat\All_App_Store\ALL_APP_STORE\Testing_bundles.xlsx")
        if "bundle_id" not in df.columns:
            raise ValueError("Excel must contain a 'bundle_id' column")

        bundle_lst = df["bundle_id"].dropna().astype(str).unique().tolist()
        validator = BundleValidator()
        val_ids = await validator.validate_and_route_ids(bundle_lst)

        # Store processors
        store_processors = {
            "apple": AppleStoreConfig(),
            "android": appstoreManager(),
            "amazon": AmazonStoreConfig(),
            "microsoft": MicrosoftManager(),
            "gallaxy": GallaxyManager(),
            "samsung": Samsung_app_store_Manager(),
            "zeasn": ZeasnManager(),
            "vizio": VizioManager(),
            "roku": rokuManager(),
            "lg": lgstoreManager()
        }

        if val_ids is None or len(val_ids) == 0:
            return

        tasks = [self.process_store(store_name, processor, val_ids) for store_name, processor in store_processors.items()]
        await asyncio.gather(*tasks)
        return



async def main_wrapper():
    config = DirectoryConfig()
    config.setup_logger()
    await config.main()
    


if __name__ == "__main__":
    asyncio.run(main_wrapper())
