# lgstore.py
import asyncio
from pathlib import Path
from asyncio import Semaphore
import pandas as pd
from loguru import logger
import sys
sys.path.append(str(Path(__file__).resolve().parents[1]))
from Final_data_all_apps import process_all_bundle_ids
from cache import append_cache
from urllib.parse import urlparse


class lgstoreManager:
    Semaphore_limit: int = 5
    batch_size: int = 100
    output_dir: Path = Path("output")
    log_dir: Path = Path("logs/lgstore.log")
    output_file: Path = Path("output/lgstore.parquet")
    lg_file_path: Path = Path(
        r"C:\work\all_app_Store\All_app_class\store_validator\LG_store\lg_all_apps_with_developer_urls_async.csv"
    )

    def __init__(self):
        self.semaphore = Semaphore(self.Semaphore_limit)
        self.output_data_list = []
        #self.failure_data_list = []

    def replace_to_parquet(self, data, file_path: Path = None, append=False):
        """Save data to parquet file"""
        if not data:
            return

        df = pd.DataFrame(data).astype("string[pyarrow]").fillna("")
        if file_path is None:
            file_path = self.output_file

        file_path.parent.mkdir(parents=True, exist_ok=True)
        if file_path.exists():
            file_path.unlink()  # delete existing file before writing

        df.to_parquet(file_path, engine="fastparquet", index=False, append=append)

    async def process(self, ids):
        """Process LG store bundle IDs"""
        try:
            # Read LG validation data
            if not self.lg_file_path.exists():
                logger.error(f"LG validation file not found: {self.lg_file_path}")
                return

            lg_df = pd.read_csv(self.lg_file_path)
            logger.info(f"LG validation data loaded with {len(lg_df)} records")

            lg_df["appId"] = lg_df["appId"].astype(str)

            results = []

            # Check each bundle_id against LG data
            for bid in ids:
                match = lg_df[lg_df["appId"] == bid]

                if not match.empty:
                    matched_row = match.iloc[0]
                    # output_data = {
                    #     "bundle_id": bid,
                    #     "developer_url": matched_row.get("Developer_URL", ""),
                    #     "link": matched_row.get("Developer_URL", ""),
                    #     "appName": matched_row.get("appName", ""),
                    #     "catName": matched_row.get("catName", ""),
                    #     "catCode": matched_row.get("catCode", ""),
                    #     "dplyDate": matched_row.get("dplyDate", ""),
                    #     "downCount": matched_row.get("downCount", ""),
                    #     "appPath": matched_row.get("appPath", ""),
                    #     "age": matched_row.get("age", ""),
                    #     "sellrUsrName": matched_row.get("sellrUsrName", ""),
                    #     "avgSscr": matched_row.get("avgSscr", ""),
                    #     "sellrUsrNo": matched_row.get("sellrUsrNo", ""),
                    #     "preFlag": matched_row.get("preFlag", ""),
                    #     "source_file": "lg_all_apps_with_developer_urls_async.csv",
                    # }

                    dict_val = {"app_name": matched_row.get("appName", ""), "bundle_id": bid,
                                    "url": matched_row.get("Developer_URL", ""), "developer_url": matched_row.get("Developer_URL", "")}
                    results.append(dict_val)

                else:
                    logger.warning(f"No match found for LG bundle ID: {bid}")

            if not results:
                logger.warning("No valid LG metadata fetched") 
                return []

            
            await process_all_bundle_ids(results)
            append_cache(results)
            logger.info(f"LG store processing completed with {len(results)} records")


        except Exception as e:
            logger.error(f"Error processing LG store: {e}")
