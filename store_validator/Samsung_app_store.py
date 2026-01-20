# CORRECTED samsung_app_store.py
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
class Samsung_app_store_Manager:
    semaphore_limit: int = 5
    batch_size: int = 100
    output_dir: Path = Path("output")
    log_dir: Path = Path("logs/samsung_app_store.log")
    output_file: Path = Path("output/samsung.parquet")
    lookup_url: str = "https://www.samsung.com/us/appstore/app/{bundle_id}/"

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

    def extract_app_id(self, content: str) -> Dict[str, Optional[str]]:
        if not content:
            return {}
            
        tree = html.fromstring(content)

        def get_meta_content(name: str) -> Optional[str]:
            result = tree.xpath(f'//meta[@name="{name}"]/@content')
            return result[0] if result else ""

        return {
            "app_store_id": get_meta_content("appstore:store_id") or "",
            "appstore_bundle_id": get_meta_content("appstore:bundle_id") or "",
            "appstore_developer_url": get_meta_content("appstore:developer_url") or "",
        }

    def write_to_parquet(self, data, file_path=None):
        if file_path is None:
            file_path = self.output_file
            
        if not data:
            return
            
        df = pd.DataFrame(data)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        df = df.astype("string[pyarrow]").fillna("")

        if not df.empty:
            df.to_parquet(file_path, engine="pyarrow", index=False)

    async def fetch_data(self, session: AsyncSession, bundle_id: str, retries: int = 3, timeout: int = 30):
        attempt = 0
        url = self.lookup_url.format(bundle_id=bundle_id)
        
        while attempt < retries:
            async with self.semaphore:
                try:
                    response = await session.get(url, timeout=timeout, impersonate="chrome", allow_redirects=True)
                    res_status_code = response.status_code
                    logger.info(f"[{res_status_code}] {url}")
                    response.raise_for_status()

                    meta_data = self.extract_app_id(response.text)
                    meta_data.update({
                        "url": url,
                        "bundle_id": bundle_id,
                        "status_code": res_status_code,
                    })

                    self.output_data_list.append(meta_data)
                    return

                except Exception as e:
                    logger.error(f"Retry {attempt + 1} failed for {url}: {e}")
                    attempt += 1
                    if attempt == retries:
                        self.output_data_list.append({
                            "url": url, 
                            "bundle_id": bundle_id, 
                            "status_code": "N/A",
                            "app_store_id": "",
                            "appstore_bundle_id": "",
                            "appstore_developer_url": ""
                        })
                    await asyncio.sleep(2)

    async def process(self, input_path: Path):
        # self.setup_logger()
        
        Path("output").mkdir(exist_ok=True)
        
        if not input_path.exists():
            logger.error(f"Input file not found: {input_path}")
            return
            
        df = pd.read_parquet(input_path)
        ids = df["bundle_id"].dropna().unique().tolist()
        
        logger.info(f"Processing {len(ids)} Samsung bundle IDs")
        
        async with AsyncSession(impersonate="chrome", allow_redirects=True) as session:
            tasks = [self.fetch_data(session, bid) for bid in ids]
            await asyncio.gather(*tasks)
        
        if self.output_data_list:
            self.write_to_parquet(self.output_data_list)
            logger.info(f"Saved {len(self.output_data_list)} Samsung results")