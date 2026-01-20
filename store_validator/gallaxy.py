# CORRECTED gallaxy.py
import asyncio
from pathlib import Path
from curl_cffi.requests import AsyncSession
from asyncio import Semaphore
import pandas as pd
from lxml import html
from typing import Dict, Optional, List
from loguru import logger
from dataclasses import dataclass, field
import sys

@dataclass
class GallaxyManager:
    semaphore_limit: int = 5
    batch_size: int = 100
    output_dir: Path = Path("output")
    log_dir: Path = Path("logs/gallaxy.log")
    output_file: Path = Path("output/gallaxy.parquet")
    lookup_url: str = "https://galaxystore.samsung.com/api/detail/{bundle_id}"

    def __post_init__(self):
        self.semaphore = Semaphore(self.semaphore_limit)
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
            return

        df = pd.DataFrame(data)
        df = df.astype("string[pyarrow]").fillna("")
        file_path.parent.mkdir(parents=True, exist_ok=True)

        if file_path.exists():
            file_path.unlink()

        df.to_parquet(file_path, engine="pyarrow", index=False)

    async def fetch_Data_app(self, session: AsyncSession, bundle_id: str, retries: int = 3) -> Dict:
        attempt = 0
        url = self.lookup_url.format(bundle_id=bundle_id)
        
        while attempt < retries:
            async with self.semaphore:
                try:
                    headers = {"User-Agent": "Mozilla/5.0"}
                    response = await session.get(url, headers=headers)
                    data = response.json()
                    logger.debug(f"Full response JSON for {bundle_id}: {data}")

                    seller = data.get("SellerInfo", {})
                    logger.debug(f"SellerInfo for {bundle_id}: {seller}")

                    result = {
                        "bundle_id": bundle_id,
                        "trade_name": seller.get("sellerTradeName", ""),
                        "site": seller.get("sellerSite", ""),
                        "address": seller.get("firstSellerAddress", ""),
                        "registration_number": seller.get("registrationNumber", ""),
                    }
                    self.output_data_list.append(result)
                    return result

                except Exception as e:
                    attempt += 1
                    logger.error(f"Retry {attempt} failed for {url}: {e}")
                    if attempt == retries:
                        self.failure_data_list.append({
                            "bundle_id": bundle_id,
                            "error": str(e)
                        })
                    await asyncio.sleep(2)
        
        return {}

    async def process(self, input_path: Path):
        # self.setup_logger()
        
        Path("output").mkdir(exist_ok=True)
        
        if not input_path.exists():
            logger.error(f"Input file not found: {input_path}")
            return
            
        df = pd.read_parquet(input_path)
        ids = df["bundle_id"].dropna().unique().tolist()

        logger.info(f"Processing {len(ids)} Gallaxy bundle IDs")
        
        async with AsyncSession() as session:
            tasks = [self.fetch_Data_app(session, app_id) for app_id in ids]
            await asyncio.gather(*tasks)

        if self.output_data_list:
            self.replace_to_parquet(self.output_data_list)
            logger.info(f"Saved {len(self.output_data_list)} Gallaxy results")
            
        if self.failure_data_list:
            Path("failure_output").mkdir(exist_ok=True)
            self.replace_to_parquet(self.failure_data_list, Path("failure_output/gallaxy_failure.parquet"))