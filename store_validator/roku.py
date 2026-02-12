# CORRECTED roku.py
import asyncio
from pathlib import Path
import pandas as pd
from loguru import logger
from typing import Dict, Optional, List
from dataclasses import dataclass, field
from asyncio import Semaphore
import sys
sys.path.append(str(Path(__file__).resolve().parents[1]))
from Final_data_all_apps import process_all_bundle_ids
from cache import append_cache
from urllib.parse import urlparse


@dataclass
class rokuManager:
    Semaphore_limit: int = 5
    batch_size: int = 100
    output_dir: Path = Path("output")
    log_dir: Path = Path("logs")
    log_file: Path = Path("logs/roku.log")
    output_file: Path = Path("output/roku.parquet")
    
    # FIXED: Use default_factory for mutable list
    csv_files: List[Path] = field(default_factory=lambda: [
        Path(r"C:\DataBeat\All_App_Store\ALL_APP_STORE\store_validator\roku_store\roku_store_searching_apps.csv"),
        Path(r"C:\DataBeat\All_App_Store\ALL_APP_STORE\store_validator\roku_store\roku_app_data_updated.csv")
    ])

    def __post_init__(self):
        self.semaphore = Semaphore(self.Semaphore_limit)
        self.output_data_list = []  
        self.failure_data_list = []

    # def setup_logger(self):
    #     logger.remove()
    #     self.log_dir.mkdir(exist_ok=True)

    #     # FIXED: Use roku.log instead of gallaxy.log
    #     self.log_file = self.log_dir / "roku.log"
    #     logger.add(
    #         self.log_file,
    #         retention="10 days",
    #         level="INFO",
    #         format="{time} {level} {file.name}:{function}:{line} - {message}",
    #         backtrace=True,
    #         diagnose=True,
    #         enqueue=True, 
    #     )
    #     logger.add(sys.stdout, level="INFO")
    #     logger.info(f"Logging started in file: {self.log_file}")
    # In roku.py - fix the replace_to_parquet method
    def replace_to_parquet(self, data, file_path=None):
        """Save data to parquet file"""
        if file_path is None:
            file_path = self.output_file

        if not data:
            # Create empty file with expected columns
            df = pd.DataFrame(
                columns=["bundle_id", "appstore_developer_url", "store_url", "appName", "source_file"]
            )
            file_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(file_path, engine="pyarrow", index=False)  # REMOVED append parameter
            return

        df = pd.DataFrame(data)
        df = df.astype("string[pyarrow]").fillna("")
        file_path.parent.mkdir(parents=True, exist_ok=True)

        if file_path.exists():
            file_path.unlink()

        df.to_parquet(file_path, engine="pyarrow", index=False)  # REMOVED append parameter

    async def process(self, ids):
        """Process Roku store bundle IDs using two CSV validation files"""
        logger.info(f"Starting Roku store processing for {len(ids)} bundle IDs")
        
        try:
            # Process both Roku CSV files
            combined_df = pd.DataFrame()
            for file in self.csv_files:
                if not file.exists():
                    logger.warning(f"ROKU validation file not found: {file}")
                    continue
                df_csv = pd.read_csv(file)
                # Clean CSV IDs
                if 'appstore_bundle_id' in df_csv.columns:
                    df_csv['appstore_bundle_id'] = df_csv['appstore_bundle_id'].astype(str).str.strip().replace(r'\.0$', '', regex=True)
                    combined_df = pd.concat([combined_df, df_csv], ignore_index=True)
                else:
                    logger.warning(f"appstore_bundle_id column not found in {file}")

            if combined_df.empty:
                logger.error("No Roku CSV data found to validate against")
                return


            results = []
            # Process each bundle ID
            for bid in ids:
                match = combined_df[combined_df['appstore_bundle_id'] == bid]
                if not match.empty:
                    matched_row = match.iloc[0]

                    dict_val = {"app_name": matched_row.get('appName', ''), "bundle_id": bid, "url": matched_row.get('url', ''),
                                "developer_url": matched_row.get('appstore_developer_url', '') }

                    results.append(dict_val)

            if not results:
                logger.warning("No valid Roku metadata fetched")
                return []
            
            append_cache(results)
            print('lengthh of roku', len(results))
            await process_all_bundle_ids(results)
            return

        except Exception as e:
            logger.error(f"Error processing Roku store: {e}")
