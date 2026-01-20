# CORRECTED vizio.py
import asyncio
from pathlib import Path
import pandas as pd
from loguru import logger
from typing import Dict, List
from dataclasses import dataclass, field
import sys

@dataclass
class VizioManager:
    vizio_file_path: Path = Path("C:\work\all_app_Store\All_app_class\store_validator\vizio_store\philips_vizio_appstoday.parquet")
    log_dir: Path = Path("logs/vizio.log")
    output_file: Path = Path("output/vizio.parquet")

    def __post_init__(self):
        self.output_data_list = []  
        self.failure_data_list = []

    # def setup_logger(self):
    #     logger.remove()
    #     self.log_dir.parent.mkdir(exist_ok=True)

    #     logger.add(
    #         self.log_dir,
    #         retention="10 days",
    #         level="INFO",
    #         format="{time} {level} {file.name}:{function}:{line} - {message}",
    #         backtrace=True,
    #         diagnose=True,
    #         enqueue=True, 
    #     )
    #     logger.add(sys.stdout, level="INFO")
    #     logger.info(f"Logging started in file: {self.log_dir}")

    def replace_to_parquet(self, data, file_path=None):
        if file_path is None:
            file_path = self.output_file
            
        if not data:
            # Create empty file with expected columns
            df = pd.DataFrame(columns=["bundle_id", "data_developer_url", "link", "data_app_name", "source_file"])
            file_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(file_path, engine="pyarrow", index=False)
            return

        df = pd.DataFrame(data)
        df = df.astype("string[pyarrow]").fillna("")
        file_path.parent.mkdir(parents=True, exist_ok=True)

        if file_path.exists():
            file_path.unlink()

        df.to_parquet(file_path, engine="pyarrow", index=False)

    async def process(self, input_path: Path):
        # self.setup_logger()
        
        Path("output").mkdir(exist_ok=True)
        
        if not input_path.exists():
            logger.error(f"Input file not found: {input_path}")
            return

        df = pd.read_parquet(input_path)
        input_ids = df["bundle_id"].dropna().unique().tolist()
        
        logger.info(f"Processing {len(input_ids)} Vizio bundle IDs")
        
        if not self.vizio_file_path.exists():
            logger.error(f"Vizio file not found: {self.vizio_file_path}")
            failure_data = [{"bundle_id": bid, "error": "Vizio validation file not found"} for bid in input_ids]
            Path("failure_output").mkdir(exist_ok=True)
            self.replace_to_parquet(failure_data, Path("failure_output/vizio_failure.parquet"))
            return
        
        vizio_df = pd.read_parquet(self.vizio_file_path)
    
        for bid in input_ids:
            match_found = False
            matched_row = None
            
            if 'data-app-id' in vizio_df.columns:
                match = vizio_df[vizio_df['data-app-id'] == bid]
                if not match.empty:
                    match_found = True
                    matched_row = match.iloc[0]
            
            if not match_found and 'data-bundle-id' in vizio_df.columns:
                match = vizio_df[vizio_df['data-bundle-id'] == bid]
                if not match.empty:
                    match_found = True
                    matched_row = match.iloc[0]
            
            if match_found and matched_row is not None:
                output_data = {
                    "bundle_id": bid,
                    "data_developer_url": matched_row.get('data-developer-url', ''),
                    "link": matched_row.get('data-developer-url', ''),
                    "data_app_name": matched_row.get('data-app-name', ''),
                    "source_file": "philips_vizio_appstoday.parquet"
                }
                self.output_data_list.append(output_data)
            else:
                failure_data = {
                    "bundle_id": bid,
                    "error": "Bundle ID not found in Vizio validation data",
                    "checked_columns": ["data-app-id", "data-bundle-id"]
                }
                self.failure_data_list =[failure_data]
        
        if self.output_data_list:
            self.replace_to_parquet(self.output_data_list)
            logger.info(f"Saved {len(self.output_data_list)} Vizio results")
        
        if self.failure_data_list:
            Path("failure_output").mkdir(exist_ok=True)
            self.replace_to_parquet(self.failure_data_list, Path("failure_output/vizio_failure.parquet"))
        
        logger.info(f"Vizio processing complete. Success: {len(self.output_data_list)}, Failures: {len(self.failure_data_list)}")
        