#Validation.py
import re
import pandas as pd
from pathlib import Path
from loguru import logger
from typing import Optional
from Final_data_all_apps import process_all_bundle_ids
from cache import load_cache
import asyncio

class BundleValidator:
    """Validates and routes bundle IDs to store-specific files with caching and logging."""

    # Constants
    STORE_PATTERNS = {
        "microsoft": r"^9[a-zA-Z0-9]{8,15}$",
        "amazon": r"^(?:/dp/)?(?=[A-Z0-9]{10}$)(?=.*[A-Z])[A-Z0-9]{10}$",
        "apple": r"(?:id)?(\d{9,})",
        "android": r"[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+",
        "zeasn": r"^\d{8}$",
        "roku":  r"^\d{4,6}$",                   ##r"^\d+$",
        "samsung": r"^G\d+$",
        "lg": r"^\d{5}$",                       ## r"^\d+$"
        "playstation": r"^UP([A-Z0-9_\-]+)",
        "gallaxy": r"[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+",
        "vizio": r"^vizio\.[a-z0-9][a-z0-9.\-\+]*$"
    }

    def __init__(self, routed_path: str = "routed_ids"):
        self.ROUTED_PATH = Path(routed_path)


    #  Main Function 
    async def validate_and_route_ids(self, bundle_lst):
        cache = load_cache()
        # print(cache,'caheeeee-11')
        results = []

        bundle_lst = [bid.strip() for bid in bundle_lst if bid]

        existing_ids = [cache[bid] for bid in bundle_lst if bid in cache.keys()]
      
        if existing_ids:
            await process_all_bundle_ids(existing_ids)

        for bid in bundle_lst:
            if bid in cache.keys():
                continue

            store = "unmatched"
            for name, pattern in self.STORE_PATTERNS.items():
                try:
                    if re.search(pattern, bid, re.I):
                        store = name
                        # print('appendinggggg----',bid)
                        results.append({"bundle_id": bid, "store": store})
                        # break                
                except re.error as e:
                    logger.error(f"Regex error for pattern {pattern}: {e}")

            

        logger.info(f'total length of bundles : {len(results)}')
        return results #[]
    

