# CORRECTED zeasn.py
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
class ZeasnManager:
    semaphore_limit: int = 5
    batch_size: int = 100
    log_dir: Path = Path("logs/zeasn.log")
    output_dir: Path = Path("output")
    output_file: Path = Path("output/zeasn.parquet")
    lookup_url: str = 'https://www.zeasn.tv/whaleeco/appstore/detail?appid={bundle_id}'

    def __post_init__(self):
        self.semaphore = Semaphore(self.semaphore_limit)
        self.output_data_list = []
        self.failure_data_list = []
        
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept": "*/*",
            "Connection": "keep-alive"
        }
        
        self.cookies = {
            "PHPSESSID": "1ehghg6d0l5frgd2is82rt6sh4"
        }

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

    @staticmethod
    def extract_appstore_(content):
        if not content:
            return {}
            
        tree = html.fromstring(content)
        
        def get_meta_content(name: str) -> Optional[str]:
            results = tree.xpath(f'//meta[@name="{name}"]/@content')
            return results[0] if results else ""
        
        return {
            "appstore_store_id": get_meta_content("appstore:store_id") or "",
            "appstore_bundle_id": get_meta_content("appstore:bundle_id") or "",
            "appstore_developer_url": get_meta_content("appstore:developer_url") or ""
        }

    async def fetch_retry_retry(self, session: AsyncSession, bundle_id: str, retries: int=3, timeout: int=30) -> dict:
        attempt = 0
        url = self.lookup_url.format(bundle_id=bundle_id)

        while attempt < retries:
            async with self.semaphore:
                try:
                    response = await session.get(
                        url,
                        timeout=timeout,
                        impersonate="chrome",
                        allow_redirects=True,
                        headers=self.headers,
                        cookies=self.cookies
                    )
                    res_status_code = response.status_code
                    logger.info(f"[{res_status_code}] {url}")
                    response.raise_for_status()
                    
                    meta_data = self.extract_appstore_(response.text)
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
                    logger.error(f"Retry {attempt+1} failed for {url}: {e}")
                    attempt += 1
                    if attempt == retries:
                        failure_result = {
                            "url": url,
                            "bundle_id": bundle_id,
                            "status_code": "N/A",
                            "appstore_store_id": "",
                            "appstore_bundle_id": "",
                            "appstore_developer_url": ""
                        }
                        self.failure_data_list=[failure_result]
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

        logger.info(f"Processing {len(ids)} Zeasn bundle IDs")
        
        async with AsyncSession(impersonate="chrome", allow_redirects=True) as session:
            tasks = [self.fetch_retry_retry(session, bid) for bid in ids]
            await asyncio.gather(*tasks)

        if self.output_data_list:
            self.replace_to_parquet(self.output_data_list)
            logger.info(f"Saved {len(self.output_data_list)} Zeasn results")
            
        if self.failure_data_list:
            Path("failure_output").mkdir(exist_ok=True)
            self.replace_to_parquet(self.failure_data_list, Path("failure_output/zeasn_failure.parquet"))