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

sys.path.append(str(Path(__file__).resolve().parents[1]))
from Final_data_all_apps import process_all_bundle_ids
from cache import append_cache

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
        
        def get_first_or_none(values):
            return values[0] if values else None
        
        return {
            "appstore_store_id": get_meta_content("appstore:store_id"),
            "appstore_bundle_id": get_meta_content("appstore:bundle_id"),
            "appstore_developer_url": get_meta_content("appstore:developer_url"),
            "appstore_app_name":  (get_first_or_none(tree.xpath('//meta[@property="og:title"]/@content'))
                                    or get_first_or_none(tree.xpath('//title/text()')))
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

    # async def fetch_with_retry(self, session: AsyncSession, bundle_id: str, retries: int = 3, timeout: int = 30) -> dict:
    #     attempt = 0
    #     url = self.lookup_url.format(bundle_id=bundle_id)
        
    #     while attempt < retries:
    #         async with self.semaphore:
    #             try:
    #                 response = await session.get(
    #                     url, 
    #                     timeout=timeout,
    #                     impersonate="chrome",
    #                     allow_redirects=True
    #                 )
    #                 logger.info(f"[{response.status_code}] {url}")
    #                 response.raise_for_status()
                    
    #                 meta_data = self.extract_appstore_meta_tags(response.text)
                    
    #                 logger.info(f"bundle id fetched for android: {bundle_id}")
    #                 dict_val = {"app_name": meta_data.get("appstore_app_name", ""), "bundle_id": bundle_id, "url": url,
    #                             "developer_url": meta_data.get("appstore_developer_url", "")}
                    
    #                 # await asyncio.sleep(1)
    #                 return dict_val
                    
    #             except Exception as e:
    #                 logger.error(f"Retry {attempt + 1} failed for {url}: {e}")
    #                 attempt += 1
    #                 await asyncio.sleep(2)
        
    #     return {}


    async def fetch_with_retry(self, session: AsyncSession, bundle_id: str, retries: int = 3, timeout: int = 30) -> dict:

        url = self.lookup_url.format(bundle_id=bundle_id)

        for attempt in range(1, retries + 1):
            async with self.semaphore:
                try:
                    response = await session.get(
                        url,
                        timeout=timeout,
                        impersonate="chrome",
                        allow_redirects=True
                    )

                    logger.info(f"[{response.status_code}] {url}")
                    response.raise_for_status()

                    meta_data = self.extract_appstore_meta_tags(response.text)

                    return {
                        "app_name": meta_data.get("appstore_app_name", ""),
                        "bundle_id": bundle_id,
                        "url": url,
                        "developer_url": meta_data.get("appstore_developer_url", "")
                    }

                except Exception as e:
                    logger.warning(
                        f"Attempt {attempt}/{retries} failed for {bundle_id}: {e}"
                    )

                    # exponential backoff (only on failure)
                    if attempt < retries:
                        await asyncio.sleep(2 ** attempt)

        # Always return None on failure (NOT {})
        return None


    async def process(self, ids):
        """Process Android store bundle IDs"""
            
        logger.info(f"Processing Android bundle IDs")
        
        async with AsyncSession(impersonate="chrome", allow_redirects=True) as session:
            tasks = [self.fetch_with_retry(session, bid) for bid in set(ids)]
            res = await asyncio.gather(*tasks)
            logger.info("fetched all bundle ids metadata from amazon store.")

            # return all_bundles_data
            valid_results = [r for r in res if r]

            if not valid_results:
                logger.warning("No valid Android bundle metadata fetched")
                return []

            #  cache write 
            append_cache(valid_results)

            #  Downstream ads.txt processing
            all_bundles_data = await process_all_bundle_ids(valid_results)

            return all_bundles_data
        

# res = appstoreManager()
# print("Starting Android store processing...")
# asyncio.run(res.process(Path(r"C:\DataBeat\All_App_Store\ALL_APP_STORE\routed_ids\android_1.parquet")))