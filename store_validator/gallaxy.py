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

sys.path.append(str(Path(__file__).resolve().parents[1]))
from Final_data_all_apps import process_all_bundle_ids
from cache import append_cache
from urllib.parse import urlparse

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

                    app_name = seller.get("sellerTradeName", "")
                    developer_url = seller.get("sellerSite", "")
                    logger.info(f"Fetched for App Name - {app_name}, Developer URL - {developer_url}")

                    dict_val = {"app_name": app_name, "bundle_id": bundle_id, "url": url, "developer_url": developer_url}

                    return dict_val

                except Exception as e:
                    attempt += 1
                    logger.error(f"Retry {attempt} failed for {url}: {e}")
                    await asyncio.sleep(2)
        
        return {}

    async def process(self, ids):
        # self.setup_logger()
        
        
        async with AsyncSession() as session:
            tasks = [self.fetch_Data_app(session, app_id) for app_id in ids]
            results = await asyncio.gather(*tasks)

        # 🔥 filter failures immediately
        valid_results = [r for r in results if r]

        if not valid_results:
            logger.warning("No valid Apple metadata fetched")
            return []

        append_cache(valid_results)
        return await process_all_bundle_ids(valid_results)