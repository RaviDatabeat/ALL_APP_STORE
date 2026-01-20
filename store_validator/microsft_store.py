# CORRECTED microsoft_store.py
import asyncio
import os 
from loguru import logger
from pathlib import Path
from curl_cffi.requests import AsyncSession
from asyncio import Semaphore
import pandas as pd
from lxml import html
from typing import Dict, Optional, List
import sys
from dataclasses import dataclass, field

@dataclass
class MicrosoftManager:
    semaphore_limit: int = 5
    batch_size: int = 100
    output_dir: Path = Path("output")
    log_dir: Path = Path("logs")
    log_file: Path = Path("logs/microsoft_store.log")
    output_file: Path = Path("output/microsoft.parquet")
    lookup_url: str = "https://apps.microsoft.com/detail/{bundle_id}?hl=en-US&gl=US"

    def __post_init__(self):
        self.semaphore = Semaphore(self.semaphore_limit)
        self.output_data_list = []
        self.failure_data_list = []
        
        self.headers = {
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9",
            "cache-control": "no-cache",
            "ms-cv": "17a9ca3b3e3a4996.15",
            "pragma": "no-cache",
            "priority": "u=1, i",
            "referer": "https://apps.microsoft.com/apps?hl=en-us&gl=US",
            "request-id": "|094a25cf6f2d4e42a5326cad6af707e0.996fce16a8d948c2",
            "sec-ch-ua": '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "traceparent": "00-094a25cf6f2d4e42a5326cad6af707e0-996fce16a8d948c2-01",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
            "x-api-ref": "db12b15d570b342c507b91e673d7053d82378cc1f7b120f479c005da86305640"
        }
        
        self.cookies = {
            "MC1": "GUID=fd86e23e42ef4dea88badb02aaa2bd42&HASH=fd86&LV=202503&V=4&LU=1742395281502",
            "exp-session-id": "11a832ed-6593-4cc0-b40a-0d15d1e8ff5e",
            "ai_user": "dOmeC9HFMzDsP5u7EASVy6|2025-04-05T22:11:21.319Z",
            "MSCC": "NR",
            "MS0": "473fba62fa264f7a89b9ba9861e48f50",
            "ai_session": "FD76DOZw4E0ddysRDDvwpo|1743891081672|1743891680694"
        }

    # def setup_logger(self):
    #     logger.remove()
    #     self.log_dir.mkdir(exist_ok=True)

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
            "appstore_developer_url": get_meta_content("appstore:developer_url") or ""
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

    async def fetch_retry_Retry(self, session: AsyncSession, bundle_id: str, retries: int=3, timeout:int=30) -> dict:
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
                    logger.error(f"Retry {attempt + 1} failed for {url}: {e}")
                    attempt += 1
                    if attempt == retries:
                        failure_result = {
                            "url": url,
                            "bundle_id": bundle_id,
                            "status_code": "N/A",
                            "appstore_store_id": None,
                            "appstore_developer_url": None
                        }
                        self.failure_data_list =failure_result
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
        
        logger.info(f"Processing {len(ids)} Microsoft bundle IDs")
        
        async with AsyncSession(impersonate="chrome", allow_redirects=True) as session:
            tasks = [self.fetch_retry_Retry(session, bid) for bid in ids]
            await asyncio.gather(*tasks)
        
        if self.output_data_list:
            self.replace_to_parquet(self.output_data_list)
            logger.info(f"Saved {len(self.output_data_list)} Microsoft results")
            
        if self.failure_data_list:
            Path("failure_output").mkdir(exist_ok=True)
            self.replace_to_parquet(self.failure_data_list, Path("failure_output/microsoft_failure.parquet"))