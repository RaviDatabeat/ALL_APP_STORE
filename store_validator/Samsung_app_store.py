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
sys.path.append(str(Path(__file__).resolve().parents[1]))
from Final_data_all_apps import process_all_bundle_ids
from cache import append_cache
from urllib.parse import urlparse


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

        def get_app_name():
            og_title = tree.xpath('//meta[@property="og:title"]/@content')
            if og_title:
                return og_title[0].replace(" on the App Store", "").strip()

            title = tree.xpath('//title/text()')
            return title[0].replace(" on the App Store", "").strip() if title else ""

        def get_meta_content(name: str) -> Optional[str]:
            result = tree.xpath(f'//meta[@name="{name}"]/@content')
            return result[0] if result else ""

        return {
            "app_name": get_app_name(),
            # "app_store_id": get_meta_content("appstore:store_id") or "",
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
                    # meta_data.update({
                    #     "url": url,
                    #     "bundle_id": bundle_id,
                    #     # "status_code": res_status_code,
                    # })

                    dict_val = {"app_name": meta_data.get("app_name",""), "bundle_id": bundle_id, "url": url,
                                            "developer_url": meta_data.get("appstore_developer_url","")}
                 
                    return dict_val

                except Exception as e:
                    logger.error(f"Retry {attempt + 1} failed for {url}: {e}")
                    attempt += 1
                    await asyncio.sleep(2)

    async def process(self, ids):
        # self.setup_logger()
        
        async with AsyncSession(impersonate="chrome", allow_redirects=True) as session:
            tasks = [self.fetch_data(session, bid) for bid in ids]
            results = await asyncio.gather(*tasks)

        # 🔥 filter failures immediately
        valid_results = [r for r in results if r]

        if not valid_results:
            logger.warning("No valid Apple metadata fetched")
            return []

        append_cache(valid_results)
        return await process_all_bundle_ids(valid_results)

        
