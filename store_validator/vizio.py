# CORRECTED vizio.py
import asyncio
from pathlib import Path
import pandas as pd
from loguru import logger
from typing import Dict, List
from dataclasses import dataclass, field
import sys
sys.path.append(str(Path(__file__).resolve().parents[1]))
from Final_data_all_apps import process_all_bundle_ids
from cache import append_cache
from urllib.parse import urlparse



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

    async def process(self, ids):
        # self.setup_logger()
        
        if not self.vizio_file_path.exists():
            logger.error(f"Vizio file not found: {self.vizio_file_path}")
            return
        
        vizio_df = pd.read_parquet(self.vizio_file_path)
    
        results = []
        for bid in ids:
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

                dict_val = {"app_name": matched_row.get('data-app-name', ''), "bundle_id": bid, "url": matched_row.get('data-developer-url', ''),
                                "developer_url": matched_row.get('data-developer-url', '')}
                
                results.append(dict_val)    

        if not results:
            logger.warning("No valid Vizio metadata fetched") 
            return []
        
        append_cache(results)
        await process_all_bundle_ids(results)
        logger.info(f"Vizio store processing completed with {len(results)} records")

        