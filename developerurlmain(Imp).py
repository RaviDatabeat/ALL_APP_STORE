# main.py
import asyncio
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

    # Permanent merged file (kept forever)
    permanent_merged_file: Path = Path("output/combined_permanent.parquet")
    # Temporary merged file (can be overwritten/deleted each run)
    temp_merged_file: Path = Path("output/combined_temp.parquet")

    def __post_init__(self):
        self.logs_dir.mkdir(exist_ok=True)
        self.store_logs_dir.mkdir(exist_ok=True)
        self.output_dir.mkdir(exist_ok=True)
        self.routed_dir.mkdir(exist_ok=True)

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

    def load_existing_developer_urls(self, merged_file: Path) -> dict:
        if merged_file.exists():
            try:
                existing_df = pd.read_parquet(merged_file)
                if not existing_df.empty and 'bundle_id' in existing_df.columns and 'developer_url' in existing_df.columns:
                    url_mapping = existing_df.set_index('bundle_id')['developer_url'].to_dict()
                    logger.info(f"Loaded {len(url_mapping)} existing developer URLs from {merged_file.name}")
                    return url_mapping
            except Exception as e:
                logger.error(f"Error reading existing merge file {merged_file.name}: {e}")
        return {}

    def merge_outputs(self, merged_output_file: Path, existing_urls=None):
    # Load existing URLs only if merging permanent file
        if merged_output_file == self.permanent_merged_file:
            existing_urls = self.load_existing_developer_urls(self.permanent_merged_file)
        elif existing_urls is None:
            existing_urls = {}

        result_frames = []
        files = list(self.output_dir.glob("*.parquet"))
        logger.info(f"Found Parquet files: {[f.name for f in files]}")

        for file in files:
            # Skip merged files themselves
            if file.name in [self.permanent_merged_file.name, self.temp_merged_file.name]:
                continue
            try:
                df = pd.read_parquet(file)
                if df.empty or 'bundle_id' not in df.columns:
                    continue

                store_name = file.stem
                dev_col_list = STORE_DEVELOPER_COLS.get(store_name, ["developer_url"])
                dev_col = next((col for col in dev_col_list if col in df.columns), None)
                df['developer_url'] = df[dev_col] if dev_col else ''

                # Fill missing from existing merged
                df['developer_url'] = df.apply(
                    lambda row: existing_urls.get(row['bundle_id'], row['developer_url']),
                    axis=1
                )

                df = df[['bundle_id', 'developer_url']]
                df["source_store"] = store_name
                result_frames.append(df)

            except Exception as e:
                logger.error(f"Error reading {file}: {e}")

        if result_frames:
            combined = pd.concat(result_frames, ignore_index=True)
            combined = combined.drop_duplicates(subset=['bundle_id'], keep="first")

            # If permanent file, merge with old data to preserve all previous bundle_ids
            if merged_output_file == self.permanent_merged_file and merged_output_file.exists():
                old_df = pd.read_parquet(merged_output_file)
                combined = pd.concat([old_df, combined], ignore_index=True)
                combined = combined.drop_duplicates(subset=['bundle_id'], keep="first")

            combined.to_parquet(merged_output_file, index=False)
            logger.success(f"Merged output saved to: {merged_output_file}")
        else:
            if not merged_output_file.exists():
                empty_df = pd.DataFrame(columns=['bundle_id', 'developer_url', 'source_store'])
                empty_df.to_parquet(merged_output_file, index=False)
                logger.warning(f"No parquet files found, created empty merged file: {merged_output_file.name}")


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

    async def validate_and_route_ids(self, df_input):
        validator = BundleValidator()
        temp_file = None

        if isinstance(df_input, pd.DataFrame):
            temp_file = self.output_dir / "temp_input.xlsx"
            df_input.to_excel(temp_file, index=False)
            df_path = temp_file
        else:
            df_path = df_input

        routed_dir = await validator.validate_and_route_ids(df_path)

        if temp_file and temp_file.exists():
            temp_file.unlink()

        return routed_dir

    async def main(self):
        if self.temp_merged_file.exists():
            self.temp_merged_file.unlink()
            logger.info("Deleted old combined_temp.parquet")
        logger.info("Starting bundle ID validation and routing")

        # Delete old routed files
        for store_name in STORE_DEVELOPER_COLS.keys():
            store_file = self.routed_dir / f"{store_name}.parquet"
            if store_file.exists():
                store_file.unlink()
                logger.info(f"Deleted old routed file: {store_file}")

        # Read Google Sheet
        df_input = self.read_google_sheet(GOOGLE_SHEET_NAME, SERVICE_ACCOUNT_FILE, GOOGLE_SHEET_ID)

        # Route IDs
        self.routed_dir = await self.validate_and_route_ids(df_input)
        logger.info(f"Routing completed. Files saved to: {self.routed_dir}")

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

        # Process each store
        for store_name, processor in store_processors.items():
            store_file = self.routed_dir / f"{store_name}.parquet"
            if store_file.exists():
                df = pd.read_parquet(store_file)
                if not df.empty:
                    logger.info(f"Processing {store_name} with {len(df)} IDs")
                    try:
                        await processor.process(store_file)
                    except Exception as e:
                        logger.error(f"Error processing {store_name}: {e}")
                else:
                    logger.info(f"No bundle IDs for {store_name}")
            else:
                logger.warning(f"{store_name} file not found at {store_file}")

        # Merge outputs
        # Merge outputs
        logger.info("Merging all outputs")

        # Temporary merged file — delete old first to start fresh
        # Temporary merged file


        self.merge_outputs(self.temp_merged_file)

        # Permanent merged file — preserve old data
        self.merge_outputs(self.permanent_merged_file)

        logger.success("All processing completed successfully")

        # Optional: remove temp file after done
        if self.temp_merged_file.exists():
            self.temp_merged_file.unlink()
            logger.info("Removed temporary merged file after processing")




async def main_wrapper():
    config = DirectoryConfig()
    config.setup_logger()
    await config.main()


if __name__ == "__main__":
    asyncio.run(main_wrapper())
