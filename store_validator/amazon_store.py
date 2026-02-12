# CORRECTED amazon.py
import asyncio
import os
from pathlib import Path
from curl_cffi.requests import AsyncSession
from asyncio import Semaphore
import pandas as pd  # FIXED: panadas → pandas
from typing import Optional, List, Any, ClassVar, Dict
from dataclasses import dataclass, field
from loguru import logger
from lxml import html
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))
from Final_data_all_apps import process_all_bundle_ids
from cache import append_cache

@dataclass
class AmazonStoreConfig:
    semaphore_limit: int = 5  # FIXED: Semaphore_limit → semaphore_limit
    batch_size: int = 100  # FIXED: batch_size → batch_size
    output_dir: Path = Path("output")
    log_dir: Path = Path("logs/amazon_app.log")
    output_file: Path = Path("output/amazon.parquet")
    
    # FIXED: Removed duplicate output_data_list declaration
    lookup_url: str = "https://www.amazon.com/dp/{bundle_id}/"
    
    
    # Your cookies and headers here...
    cookies = {
    'session-id': '137-4243740-6564334',
    'session-id-time': '2082787201l',
    'i18n-prefs': 'USD',
    'sp-cdn': '"L5Z9:IN"',
    'ubid-main': '131-3795383-2526005',
    'lc-main': 'en_US',
    'session-token': 'yxKGfRHPUziaMXF1HCtVL/klzs1L6aqviXxCsdEN+cWyk/2iPq9gp6niZFCjvBf3aMJ5vVoiY6/yphpusp3CdYxibUXSN0v4ypW9tEoA3hsoWYKpJtRTp5dNpnVqQMDJkCrqdyUpJEZ+bT0xYq2kUh2hwNLldC1h567IP2FerhcuqTbfw3o7vbrdDc5eMl9x/D26bWozIXPv240W2YPNhZhMcHKLUI5iZ1bgjitdHyMS5NBFjoHj157Qwjl9WOOscw9Ox6/khdyxtyresBQ5NtwpZUlWl3h/rWOX+wC3F8tlCLIX8ASl15zfa8vgH8ARf56QKy0Dfa8OUp50B2C+FNdly+CTPOaN',
    'csm-hit': 'tb:6VGD1Y50GXKQKE63X4R6+s-6VGD1Y50GXKQKE63X4R6|1753184248132&t:1753184248132&adb:adblk_no',
    'rxc': 'ANRcICl09mwA8JchlPc',
    }

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'max-age=0',
        'device-memory': '8',
        'downlink': '10',
        'dpr': '1.875',
        'ect': '4g',
        'priority': 'u=0, i',
        'referer': 'https://chatgpt.com/',
        'rtt': '50',
        'sec-ch-device-memory': '8',
        'sec-ch-dpr': '1.875',
        'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-ch-ua-platform-version': '"15.0.0"',
        'sec-ch-viewport-width': '1024',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'cross-site',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
        'viewport-width': '1024',
        # 'cookie': 'session-id=137-4243740-6564334; session-id-time=2082787201l; i18n-prefs=USD; sp-cdn="L5Z9:IN"; ubid-main=131-3795383-2526005; lc-main=en_US; session-token=yxKGfRHPUziaMXF1HCtVL/klzs1L6aqviXxCsdEN+cWyk/2iPq9gp6niZFCjvBf3aMJ5vVoiY6/yphpusp3CdYxibUXSN0v4ypW9tEoA3hsoWYKpJtRTp5dNpnVqQMDJkCrqdyUpJEZ+bT0xYq2kUh2hwNLldC1h567IP2FerhcuqTbfw3o7vbrdDc5eMl9x/D26bWozIXPv240W2YPNhZhMcHKLUI5iZ1bgjitdHyMS5NBFjoHj157Qwjl9WOOscw9Ox6/khdyxtyresBQ5NtwpZUlWl3h/rWOX+wC3F8tlCLIX8ASl15zfa8vgH8ARf56QKy0Dfa8OUp50B2C+FNdly+CTPOaN; csm-hit=tb:6VGD1Y50GXKQKE63X4R6+s-6VGD1Y50GXKQKE63X4R6|1753184248132&t:1753184248132&adb:adblk_no; rxc=ANRcICl09mwA8JchlPc',
    }
    
    def __post_init__(self):
        self.semaphore = Semaphore(self.semaphore_limit)  # ADDED: Create semaphore instance
        self.output_data_list = []  # INSTANCE variable, not ClassVar
        self.failure_data_list = []

    # def setup_logger(self):
    #     logger.remove()
    #     self.log_dir.parent.mkdir(exist_ok=True)  # FIXED: Create parent directory

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

    # In amazon.py - Fix the extract_appstore_ method
    @staticmethod
    def extract_appstore_(content):
        if not content:
            return {}
            
        try:
            tree = html.fromstring(content)

            og_title = tree.xpath('//meta[@property="og:title"]/@content')
            title = og_title[0] if og_title else (
                tree.xpath('//title/text()')[0]
                if tree.xpath('//title/text()') else ""
            )

            def get_meta_content(name: str) -> Optional[str]:
                results = tree.xpath(f'//meta[@name="{name}"]/@content')
                return results[0] if results else ""
            
            meta_data = {
                "appstore_store_id": get_meta_content("appstore:store_id") or "",
                "appstore_bundle_id": get_meta_content("appstore:bundle_id") or "",
                "appstore_developer_url": get_meta_content("appstore:developer_url") or "",
                "app_name": title.replace(" on the App Store", "").strip(),
            }

            return meta_data
        except Exception as e:
            logger.error(f"Error parsing Amazon content: {e}")
            return {}

    def replace_to_parquet(self, data, append=False):
        """Save data to parquet file"""
        if not data:
            return
            
        df = pd.DataFrame(data)
        df = df.astype("string[pyarrow]").fillna("")
        file_path = self.output_file
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        if  file_path.exists():
            file_path.unlink()

        df.to_parquet(file_path, engine="pyarrow", index=False)  # FIXED: Use pyarrow engine

    

    # async def fetch_meta_Data(self, session: AsyncSession, bundle_id: str, retries: int = 2, timeout: int = 30) -> dict:
    #     """Fetch metadata with retries"""
    #     url = self.lookup_url.format(bundle_id=bundle_id)
    #     attempt = 0  # FIXED: Added missing attempt variable
        
    #     while attempt < retries:
    #         async with self.semaphore:  # FIXED: Use instance semaphore
    #             try:
    #                 response = await session.get(
    #                     url,
    #                     timeout=timeout,
    #                     impersonate="chrome",
    #                     allow_redirects=True,
    #                     headers=self.headers,
    #                     cookies=self.cookies
    #                 )
    #                 logger.info(f"[{response.status_code}] {url}")
    #                 response.raise_for_status()

    #                 meta_data = self.extract_appstore_(response.text)
                    
    #                 logger.info(f"bundle id fetched for amazon: {bundle_id}")
    #                 dict_val = {"app_name": meta_data.get("app_name"), "bundle_id": bundle_id, "url": url,
    #                                             "developer_url": meta_data.get("appstore_developer_url")}
    #                 # await asyncio.sleep(1)
    #                 return dict_val
                    
                    
    #             except Exception as e:
    #                 logger.error(f"Retry {attempt + 1} failed for {url}: {e}")
    #                 attempt += 1

    async def fetch_meta_Data(self, session: AsyncSession, bundle_id: str, retries: int = 2, timeout: int = 30) -> dict:

        url = self.lookup_url.format(bundle_id=bundle_id)

        for attempt in range(1, retries + 1):
            try:
                async with self.semaphore:
                    response = await session.get(
                        url,
                        timeout=timeout,
                        impersonate="chrome",
                        allow_redirects=True,
                        headers=self.headers,
                        cookies=self.cookies,
                    )

                logger.info(f"[{response.status_code}] {url}")
                response.raise_for_status()

                meta = self.extract_appstore_(response.text)

                return {
                    "app_name": meta.get("app_name", ""),
                    "bundle_id": bundle_id,
                    "url": url,
                    "developer_url": meta.get("appstore_developer_url", ""),
                }

            except Exception as e:
                logger.warning(
                    f"Attempt {attempt}/{retries} failed for {bundle_id}: {e}"
                )

                if attempt < retries:
                    await asyncio.sleep(2 ** attempt)  # exponential backoff

        return None


    async def process(self, ids):  # FIXED: Corrected parameter order
        """Process Amazon store bundle IDs"""
        
        async with AsyncSession(impersonate="chrome", allow_redirects=True) as session:
            tasks = [self.fetch_meta_Data(session, bid) for bid in set(ids)]
            res = await asyncio.gather(*tasks)
            logger.info("fetched all bundle ids metadata from amazon store.")
        
            valid_results = [r for r in res if r]

            if not valid_results:
                logger.warning("No valid Amazon bundle metadata fetched")
                return []

            #  cache write 
            append_cache(valid_results)

            #  Downstream ads.txt processing
            all_bundles_data = await process_all_bundle_ids(valid_results)

            return all_bundles_data

            


# res = AmazonStoreConfig()
# ids = ["b09lhkflm6", "B00D6P1OOW", "B00BHDIS12", "B07JYDMWQM"]  # Example bundle IDs for testing
# asyncio.run(res.process(ids))
