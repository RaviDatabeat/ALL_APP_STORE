# CORRECTED apple.py
import asyncio
from pathlib import Path
from curl_cffi.requests import AsyncSession
from asyncio import Semaphore
import pandas as pd
from lxml import html
from typing import Dict, Optional, List, Any
from dataclasses import dataclass, field
from loguru import logger
import sys
sys.path.append(str(Path(__file__).resolve().parents[1]))
from Final_data_all_apps import process_all_bundle_ids
from cache import append_cache
from urllib.parse import urlparse


@dataclass
class AppleStoreConfig:
    """Configuration for Apple Store validator"""
    semaphore_limit: int = 5
    batch_size: int = 100
    output_dir: Path = Path("output")
    log_dir: Path = Path("logs")
    log_file: Path = Path("logs/apple_app.log")
    output_file: Path = Path("output/apple.parquet")  # FIXED: Changed from gallaxy.parquet
    
    # API endpoints
    lookup_url: str = "https://itunes.apple.com/lookup?id={bundle_id}"
    appstore_url: str = "https://apps.apple.com/app/id{track_id}"
    
    # Required columns for schema normalization
    required_columns: List[str] = field(default_factory=lambda: [
        "trackId", "bundleId", "trackName", "artistName",
        "averageUserRating", "userRatingCount", "sellerUrl",
        "appstore_store_id", "appstore_bundle_id", "appstore_developer_url",
        "developerWebsite", "appSupportUrl", "privacyPolicyUrl"
    ])

    def __post_init__(self):
        self.semaphore = Semaphore(self.semaphore_limit)
        self.output_data_list = []
        self.failure_data_list = []

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

    def normalize_schema(self, data: dict) -> dict:
        """Ensure every row has the same columns."""
        return {col: data.get(col, "") for col in self.required_columns}

    @staticmethod
    def extract_meta_tags(content: str) -> Dict[str, Optional[str]]:
        """Extract App Store meta + body links (developer, support, privacy)."""
        tree = html.fromstring(content)

        def get_meta_content(name: str) -> Optional[str]:
            result = tree.xpath(f'//meta[@name="{name}"]/@content')
            return result[0] if result else None
        
        developer_privacy_policy = tree.xpath('//a[@aria-label="Developer’s Privacy Policy"]/@href')
        parsed = urlparse(developer_privacy_policy[0]) if developer_privacy_policy else None
        dev_url = f"{parsed.scheme}://{parsed.netloc}" if parsed and parsed.scheme and parsed.netloc else ""

        # meta tags
        meta_data = {
            "appstore_store_id": get_meta_content("appstore:store_id"),
            "appstore_bundle_id": get_meta_content("appstore:bundle_id"),
            "appstore_developer_url": get_meta_content("appstore:developer_url"),
            "developerPrivacyPolicyUrl": dev_url
        }

        # page body links
        # developer_website = tree.xpath('//a[contains(text(), "Developer Website")]/@href')
        # app_support = tree.xpath('//a[contains(text(), "App Support")]/@href')
        # privacy_policy = tree.xpath('//a[contains(text(), "Privacy Policy")]/@href')

        return meta_data

    def append_to_parquet(self, data: List[Dict[str, Any]]):
        """Append normalized data efficiently using pyarrow."""
        if not data:
            return

        # normalize schema
        normalized_data = [self.normalize_schema(d) for d in data]

        df = pd.DataFrame(normalized_data).astype("string[pyarrow]").fillna("")
        self.output_file.parent.mkdir(parents=True, exist_ok=True)

        # if self.output_file.exists():
        #     existing_df = pd.read_parquet(self.output_file)
        #     combined_df = pd.concat([existing_df, df], ignore_index=True)
        #     combined_df.to_parquet(self.output_file, engine="pyarrow", index=False)
        # else:
        df.to_parquet(self.output_file, engine="pyarrow", index=False)

    async def fetch_json_metadata(self, session: AsyncSession, bundle_id: str) -> dict:
        """Fetch app metadata (JSON) from iTunes Lookup API."""
        async with self.semaphore:
            url = self.lookup_url.format(bundle_id=bundle_id)
            response = await session.get(url, impersonate="chrome", allow_redirects=True)
            response.raise_for_status()
            results = response.json().get("results", [])
            if results:
                meta = results[0]
                return {
                    "trackId": meta.get("trackId"),
                    # "bundleId": meta.get("bundleId"),
                    "bundleId": bundle_id,
                    "trackName": meta.get("trackName"),
                    "artistName": meta.get("artistName"),
                    # "averageUserRating": meta.get("averageUserRating"),
                    # "userRatingCount": meta.get("userRatingCount"),
                    "sellerUrl": meta.get("sellerUrl"), 
                }
            return {}

    async def fetch_html_metadata(self, session: AsyncSession, track_id: str) -> dict:
        """Fetch developer URL (HTML page) from App Store."""
        async with self.semaphore:
            url = self.appstore_url.format(track_id=track_id)
            response = await session.get(url, impersonate="chrome", allow_redirects=True)
            response.raise_for_status()
            return self.extract_meta_tags(response.text)
       
    
    async def organizing_data(self, data: dict):
        bundle_id = data.get("bundleId")
        seller_url = data.get("sellerUrl")
        url = (self.lookup_url.format(bundle_id=bundle_id) if seller_url else self.appstore_url.format(track_id=data.get("trackId")))

        dict_info = {
            "app_name": data.get("trackName"),
            "bundle_id": bundle_id,
            "url": url,
            "developer_url": seller_url or data.get("appstore_developer_url") or data.get("developerPrivacyPolicyUrl")}
        
        # res = await process_all_bundle_ids(dict_info)
        return dict_info


    # async def fetch_with_merge(self, session: AsyncSession, bundle_id: str,retries:int=3,) -> dict:
    #     """Fetch JSON + HTML metadata and merge results."""
    #     try:
    #         json_meta = await self.fetch_json_metadata(session, bundle_id)
    #         if not json_meta:
    #             logger.warning(f"No JSON metadata for {bundle_id}")
    #             return {}

    #         track_id = json_meta.get("trackId")
    #         html_meta = {}
    #         if track_id:  # fetch HTML only if we have trackId
    #             try:
    #                 html_meta = await self.fetch_html_metadata(session, str(track_id))
    #             except Exception as e:
    #                 logger.error(f"Failed to fetch HTML for {bundle_id}: {e}")

    #         merged = {**json_meta, **html_meta}
    #         data_val = await self.organizing_data(merged)

    #         # return self.normalize_schema(merged)
    #         return data_val
        

    #     except Exception as e:
    #         logger.error(f"Failed to fetch metadata for {bundle_id}: {e}")
    #         attempt = 0
    #         while attempt < retries:
    #             attempt+=1
    #             if attempt== retries:
    #                 failure_result= {
    #                         "trackId": self.lookup_url,
    #                         "bundleId": bundle_id,
    #                         "trackName":None,
    #                         "artistName": None,
    #                         "averageUserRating": None,
    #                         "userRatingCount": None,
    #                         "sellerUrl": None

    #                 }
    #                 self.failure_data_list=[failure_result]
    #     return {}


    async def fetch_with_merge(self, session: AsyncSession, bundle_id: str, retries: int = 3) -> dict | None:
        for attempt in range(1, retries + 1):
            try:
                json_meta = await self.fetch_json_metadata(session, bundle_id)
                if not json_meta:
                    return None

                html_meta = {}
                track_id = json_meta.get("trackId")
                if track_id:
                    try:
                        html_meta = await self.fetch_html_metadata(session, str(track_id))
                    except Exception as e:
                        logger.warning(f"HTML fetch failed for {bundle_id}: {e}")

                merged = {**json_meta, **html_meta}
                return await self.organizing_data(merged)

            except Exception as e:
                logger.warning(f"Attempt {attempt}/{retries} failed for {bundle_id}: {e}")
                if attempt < retries:
                    await asyncio.sleep(2 ** attempt)

        return None


    async def process(self, ids):
        """Process Apple store bundle IDs"""
        
        async with AsyncSession(impersonate="chrome", allow_redirects=True) as session:
            session.headers["accept"] = "application/json"
            tasks = [self.fetch_with_merge(session, str(app_id)) for app_id in ids]
            results = await asyncio.gather(*tasks)


            # logger.info("fetched all bundle ids metadata from apple store.")
            
            # 🔥 filter failures immediately
            valid_results = [r for r in results if r]

            if not valid_results:
                logger.warning("No valid Apple metadata fetched")
                return []

            append_cache(valid_results)
            print('lengthh of apple', len(valid_results))
            return await process_all_bundle_ids(valid_results)





# testing = AppleStoreConfig()
# res = asyncio.run(testing.process(["6444051625"]))
# # res = asyncio.run(testing.process(["6444051682", "1168117279"]))
# print(len(res))
            
            


