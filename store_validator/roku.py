# CORRECTED roku.py
import asyncio
from pathlib import Path
import pandas as pd
from loguru import logger
from typing import Dict, Optional, List
from dataclasses import dataclass, field
from asyncio import Semaphore
import sys

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
        Path(r"C:\work\all_app_Store\All_app_class\store_validator\roku_store\roku_store_searching_apps.csv"),
        Path(r"C:\work\all_app_Store\All_app_class\store_validator\roku_store\roku_app_data_updated.csv")
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

    async def process(self, input_path: Path):
        """Process Roku store bundle IDs using two CSV validation files"""
        # self.setup_logger()
        logger.info(f"Starting Roku store processing for {input_path}")
        
        try:
            Path("output").mkdir(exist_ok=True)
            Path("failure_output").mkdir(exist_ok=True)

            if not input_path.exists():
                logger.error(f"Input file not found: {input_path}")
                return

            # Read input IDs
            df = pd.read_parquet(input_path)
            input_ids = df["bundle_id"].dropna().astype(str).str.strip().tolist()
            input_ids = [bid.replace(".0", "") for bid in input_ids]

            logger.info(f"Found {len(input_ids)} bundle IDs to process")

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
                failure_data = [{"bundle_id": bid, "error": "No Roku CSV data found"} for bid in input_ids]
                self.replace_to_parquet(failure_data, Path("failure_output/roku_failure.parquet"))
                return

            # Process each bundle ID
            for bid in input_ids:
                match = combined_df[combined_df['appstore_bundle_id'] == bid]
                if not match.empty:
                    matched_row = match.iloc[0]
                    self.output_data_list.append({
                        "bundle_id": bid,
                        "appstore_developer_url": matched_row.get('appstore_developer_url', ''),
                        "store_url": matched_row.get('url', ''),
                        "appName": matched_row.get('appName', ''),
                        "source_file": "combined_roku_files"  # FIXED: Removed file.name reference
                    })
                else:
                    self.failure_data_list.append({
                        "bundle_id": bid,
                        "error": "Bundle ID not found in Roku validation data",
                        "checked_column": "appstore_bundle_id"
                    })

            # Save results
            if self.output_data_list:
                self.replace_to_parquet(self.output_data_list, Path("output/roku.parquet"))
            
            if self.failure_data_list:
                self.replace_to_parquet(self.failure_data_list, Path("failure_output/roku_failure.parquet"))

            logger.info(f"Roku processing complete. Success: {len(self.output_data_list)}, Failures: {len(self.failure_data_list)}")
            print(f"Roku processing complete. Success: {len(self.output_data_list)}, Failures: {len(self.failure_data_list)}")

            return len(self.output_data_list)

        except Exception as e:
            logger.error(f"Error processing Roku store: {e}")
            #raise ValueError(f"Error processing Roku  store: {e}")