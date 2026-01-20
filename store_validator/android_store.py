# CORRECTED android.py
import asyncio
import os
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
class appstoreManager:
    semaphore_limit: int = 5
    batch_size: int = 100
    output_dir: Path = Path("output")
    log_dir: Path = Path("logs/android_app.log")
    output_file: Path = Path("output/android.parquet")
    lookup_url: str = "https://play.google.com/store/apps/details?id={bundle_id}"

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
    
    @staticmethod
    def extract_appstore_meta_tags(content) -> Dict[str, Optional[str]]:
        tree = html.fromstring(content)

        def get_meta_content(name: str) -> Optional[str]:
            result = tree.xpath(f'//meta[@name="{name}"]/@content')
            return result[0] if result else None
        
        return {
            "appstore_store_id": get_meta_content("appstore:store_id"),
            "appstore_bundle_id": get_meta_content("appstore:bundle_id"),
            "appstore_developer_url": get_meta_content("appstore:developer_url")
        }

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

    async def fetch_with_retry(self, session: AsyncSession, bundle_id: str, retries: int = 3, timeout: int = 30) -> dict:
        attempt = 0
        url = self.lookup_url.format(bundle_id=bundle_id)
        
        while attempt < retries:
            async with self.semaphore:
                try:
                    response = await session.get(
                        url, 
                        timeout=timeout,
                        impersonate="chrome",
                        allow_redirects=True
                    )
                    res_status_code = response.status_code
                    logger.info(f"[{res_status_code}] {url}")
                    response.raise_for_status()
                    
                    meta_data = self.extract_appstore_meta_tags(response.text)
                    result = {
                        "url": url,
                        "bundle_id": bundle_id,
                        "status_code": res_status_code,
                        **meta_data
                    }

                    self.output_data_list.append(result)
                    
                    if len(self.output_data_list) >= self.batch_size:
                        self.replace_to_parquet(self.output_data_list)
                        self.output_data_list = []
                    
                    await asyncio.sleep(2)
                    return result
                    
                except Exception as e:
                    logger.error(f"Retry {attempt + 1} failed for {url}: {e}")
                    attempt += 1
                    if attempt == retries:
                        failure_result = {
                            "url": url,
                            "bundle_id": bundle_id,
                            "status_code": "N/A",
                            "appstore_store_id": None,
                            "appstore_bundle_id": None,
                            "appstore_developer_url": None
                        }
                        self.failure_data_list=[failure_result]
                    await asyncio.sleep(2)
        
        return {}

    async def process(self, input_path: Path):
        """Process Android store bundle IDs"""
        # self.setup_logger()
        
        Path("output").mkdir(exist_ok=True)
        
        if not input_path.exists():
            logger.error(f"Input file not found: {input_path}")
            return
            
        df = pd.read_parquet(input_path)
        ids = df["bundle_id"].dropna().unique().tolist()

        logger.info(f"Processing {len(ids)} Android bundle IDs")
        
        async with AsyncSession(impersonate="chrome", allow_redirects=True) as session:
            tasks = [self.fetch_with_retry(session, bid) for bid in ids]
            await asyncio.gather(*tasks)
        
        # Write any remaining data
        if self.output_data_list:
            self.replace_to_parquet(self.output_data_list)
            logger.success(f"Android Store data written with {len(self.output_data_list)} records")
            
        if self.failure_data_list:
            Path("failure_output").mkdir(exist_ok=True)
            self.replace_to_parquet(self.failure_data_list, Path("failure_output/android_failure.parquet"))
            logger.info(f"Saved {len(self.failure_data_list)} failed results")