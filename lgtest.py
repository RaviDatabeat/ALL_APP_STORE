#lgstore.py
import asyncio
from pathlib import Path
from curl_cffi.requests import AsyncSession
from asyncio import Semaphore
import pandas as pd
from lxml import html
from typing import Dict, Optional
from loguru import logger
from typing import Optional,List,Any,ClassVar,Dict
from dataclasses import dataclass,field
from loguru import logger
from lxml import html
import sys


class lgstoreManager:
    Semaphore_limit :int =5
    batch_size :int= 100
    output_dir :Path=Path("output")
    log_dir:Path=Path("logs/lgstore.log")
    output_file:Path=Path("output/lgstore.parquet")
    output_data_list: ClassVar[list] = []   
    lg_file_path:Path =  Path(r"C:\work\all_app_Store\All_app_class\store_validator\LG_store\lg_all_apps_with_developer_urls_async.csv")

    # def setup_logger(self):
    #     logger.remove()
    #     self.log_dir.mkdir(exist_ok=True)

    #     self.log_file = self.log_dir / "samsung_app_store.log"
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
    
    def __init__(self):
        self.semaphore = Semaphore(self.Semaphore_limit)  # ADDED: Create semaphore instance
        self.output_data_list = []  
        self.failure_data_list = []

    def replace_to_parquet(self,data, append=False):
    #Save data to parquet file"""
        file_path = self.output_file
        if not data:
            return
            
        df = pd.DataFrame(data)
        df = df.astype("string[pyarrow]").fillna("")

        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        if  file_path.exists():
            file_path.unlink()  # delete existing file before writing

        df.to_parquet(file_path, engine="fastparquet", index=False, append=append)

    async def process(self,input_path: Path):
        #Process LG store bundle IDs"""
        # self.setup_logger()
        logger.info(f"Starting LG store processing for {input_path}")
            
        try:
            # Ensure output directory exists
            Path("output").mkdir(exist_ok=True)
                
                # Read input data
            if not input_path.exists():
                logger.error(f"Input file not found: {input_path}")
                return
                    
            df = pd.read_parquet(input_path)
            input_ids = df["bundle_id"].dropna().unique().tolist()
                
            logger.info(f"Found {len(input_ids)} bundle IDs to process")
                
                # Read LG validation data from CSV
                
            if not self.lg_file_path.exists():
                logger.error(f"LG validation file not found: {self.lg_file_path}")
                    # Handle all as failures since we can't validate
                failure_data = [{"bundle_id": bid, "error": "LG validation file not found"} 
                                for bid in input_ids]
                Path("failure_output").mkdir(exist_ok=True)
                self.replace_to_parquet(failure_data, Path("failure_output/lg_failure.parquet"))
                return
                
                # Read CSV file
            lg_df = pd.read_csv(self.lg_file_path)
            logger.info(f"LG validation data loaded with {len(lg_df)} records")
                
                # Prepare output and failure lists
            lg_df['appId'] = lg_df['appId'].astype(str)
                
                # Check each bundle_id against LG data
            for bid in input_ids:
                    # Check if bundle_id exists in appId column


                match = lg_df[lg_df['appId'] == bid]
                    
                if not match.empty:
                    matched_row = match.iloc[0]
                        # Extract all data from matched row
                    output_data = {
                            "bundle_id": bid,
                            "developer_url": matched_row.get('Developer_URL', ''),
                            "link": matched_row.get('Developer_URL', ''),
                            "appName": matched_row.get('appName', ''),
                            "catName": matched_row.get('catName', ''),
                            "catCode": matched_row.get('catCode', ''),
                            "dplyDate": matched_row.get('dplyDate', ''),
                            "downCount": matched_row.get('downCount', ''),
                            "appPath": matched_row.get('appPath', ''),
                            "age": matched_row.get('age', ''),
                            "sellrUsrName": matched_row.get('sellrUsrName', ''),
                            "avgSscr": matched_row.get('avgSscr', ''),
                            "sellrUsrNo": matched_row.get('sellrUsrNo', ''),
                            "preFlag": matched_row.get('preFlag', ''),
                            "source_file": "lg_all_apps_with_developer_urls_async.csv"
                        }
                    self.output_data_list.append(output_data)
                else:
                    # Bundle ID not found in LG data - mark as failure
                    failure_data = {
                            "bundle_id": bid,
                            "error": "Bundle ID not found in LG validation data",
                            "checked_column": "appId"
                        }
                    self.failure_data_list.append(failure_data)
                
                # Save successful matches
                if self.output_data_list:
                    self.replace_to_parquet(self.output_data_list, Path("output/lg.parquet"))
                    logger.info(f"Saved {len(self.output_data_list)} successful matches to output/lg.parquet")
                
                # Save failures
                if self.failure_data_list:
                    Path("failure_output").mkdir(exist_ok=True)
                    self.replace_to_parquet(self.failure_data_list, Path("failure_output/lg_failure.parquet"))
                    logger.info(f"Saved {len(self.failure_data_list)} failures to failure_output/lg_failure.parquet")
                
                logger.info(f"LG processing complete. Success: {len(self.output_data_list)}, Failures: {len(self.failure_data_list)}")
                return len(self.output_data_list)
                
        except Exception as e:
            logger.error(f"Error processing LG store: {e}")
            #raise ValueError(f"Error processing LG store: {e}")
