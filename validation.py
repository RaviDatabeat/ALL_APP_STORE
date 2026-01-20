#Validation.py
import re
import pandas as pd
from pathlib import Path
from loguru import logger
from typing import Optional

class BundleValidator:
    """Validates and routes bundle IDs to store-specific files with caching and logging."""

    # Constants
    STORE_PATTERNS = {
        "microsoft": r"^9[a-zA-Z0-9]{8,15}$",
        "amazon": r"^(?:/dp/)?(?=[A-Z0-9]{10}$)(?=.*[A-Z])[A-Z0-9]{10}$",
        "apple": r"(?:id)?(\d{6,})",
        "android": r"[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+",
        "zeasn": r"^\d{8}$",
        "roku": r"^\d+$",
        "samsung": r"^G\d+$",
        "lg": r"^\d+$",
        "playstation": r"/product/([A-Z0-9_\-]+)",
        "gallaxy": r"[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+",
        "vizio": r"^vizio\.[a-z0-9][a-z0-9.\-\+]*$"
    }

    def __init__(self, cache_path: str = "routed_ids_cache/bundle_cache.parquet",
                 routed_path: str = "routed_ids", log_dir: str = "logs"):
        """Initialize the validator, ensuring directories and log setup."""
        self.CACHE_PATH = Path(cache_path)
        self.ROUTED_PATH = Path(routed_path)
        self.LOG_DIR = Path(log_dir)

        # Ensure folders exist
        self.CACHE_PATH.parent.mkdir(exist_ok=True, parents=True)
        self.ROUTED_PATH.mkdir(exist_ok=True, parents=True)
        self.LOG_DIR.mkdir(exist_ok=True)

        # Logger setup
        logger.add(self.LOG_DIR / "validation.log", rotation="10 days", level="INFO")
    
# In validation.py, fix the extract_id method:
    def extract_id(self, text: str, pattern: str) -> Optional[str]:  # FIXED: Added return type
        match = re.search(pattern, text)
        if match:
            try:
                return match.group(1) if match.groups() else match.group(0)  # FIXED: Handle no groups
            except IndexError:
                logger.error(f"No group(1) in regex match for: {text} using pattern: {pattern}")
        return None
    def load_cache(self) -> pd.DataFrame:
        if self.CACHE_PATH.exists():
            return pd.read_parquet(self.CACHE_PATH)
        return pd.DataFrame(columns=["bundle_id", "store"])
    def save_cache(self, df: pd.DataFrame):
    # Load existing cache
        if self.CACHE_PATH.exists():
            existing_df = pd.read_parquet(self.CACHE_PATH)
            df = pd.concat([existing_df, df], ignore_index=True)

        # Drop duplicates by bundle_id and store
        df = df.drop_duplicates(subset=["bundle_id", "store"], keep="last")

        # Save back to cache
        df.to_parquet(self.CACHE_PATH, index=False)


    #  Main Function 
    async def validate_and_route_ids(self,excel_path: str) -> Path:
        print(f" Reading Excel file: {excel_path}")
        logger.info(f"Reading Excel file: {excel_path}")
        
        df = pd.read_excel(excel_path)

        if "bundle_id" not in df.columns:
            error_msg = "Missing 'bundle_id' column"
            print(f" {error_msg}")
            logger.error(error_msg)
            raise ValueError("Excel must contain a 'bundle_id' column")

        bundle_ids = df["bundle_id"].dropna().astype(str).unique()
        current_ids = set(bundle_ids)
        print(f" Found {len(bundle_ids)} unique bundle IDs to process")
        logger.info(f"Found {len(bundle_ids)} unique bundle IDs")
        
        cache_df = self.load_cache()
        print(f" Cache contains {len(cache_df)} previously routed IDs")
        logger.info(f"Cache contains {len(cache_df)} previously routed IDs")

        new_entries = []
        store_counts = {store: 0 for store in self.STORE_PATTERNS.keys()}
        store_counts["unmatched"] = 0

        print("\n Analyzing bundle IDs:")
        print("-" * 60)
        logger.info("Starting bundle ID analysis")
        
        for bid in bundle_ids:
            bid_clean = bid.strip()
            
            # Check cache first
            cached = cache_df[cache_df["bundle_id"] == bid_clean]
            if not cached.empty:
                store_name = cached.iloc[0]["store"]
                print(f"     {bid_clean:<20} -> {store_name.upper()} (from cache)")
                logger.debug(f"ID {bid_clean} routed to {store_name} (from cache)")
                store_counts[store_name] += 1
                continue

            matched = False
            for store, pattern in self.STORE_PATTERNS.items():
                try:
                    if re.search(pattern, bid_clean, re.IGNORECASE):
                        new_entries.append({"bundle_id": bid_clean, "store": store})
                        store_counts[store] += 1
                        matched = True
                        print(f"    {bid_clean:<20} -> {store.upper()}")
                        logger.info(f"ID {bid_clean} routed to {store}")
                        #break
                except Exception as e:
                    logger.error(f"Regex error for pattern {pattern}: {e}")

            if not matched:
                store_counts["unmatched"] += 1
                print(f"    {bid_clean:<20} -> UNMATCHED")
                logger.warning(f"No store matched for: {bid_clean}")

        # Print summary
        print("\n" + "=" * 60)
        print(" ROUTING SUMMARY:")
        print("=" * 60)
        for store, count in store_counts.items():
            if count > 0:
                print(f"   {store.upper():<15}: {count:>3} IDs")
        print("=" * 60)
        
        # Log summary
        logger.info("Routing summary:")
        for store, count in store_counts.items():
            if count > 0:
                logger.info(f"  {store}: {count} IDs")

        # Update and save cache
        if new_entries:
            new_df = pd.DataFrame(new_entries)
            combined_df = pd.concat([cache_df, new_df], ignore_index=True).drop_duplicates()
            self.save_cache(combined_df)
            print(f" Saved {len(new_entries)} new entries to cache")
            logger.info(f"Saved {len(new_entries)} new entries to cache")
        else:
            print("ℹ  No new bundle IDs found to add to cache")
            logger.info("No new bundle IDs found to add to cache")
            combined_df = cache_df

        # Ensure all store files exist
        print(f"\nSaving routed IDs to store files...")
        logger.info("Saving routed IDs to store files")
        
        for store in self.STORE_PATTERNS.keys():
            out_file = self.ROUTED_PATH / f"{store}.parquet"
            store_ids = combined_df[
            (combined_df["store"] == store) &
            (combined_df["bundle_id"].isin(current_ids))
        ]["bundle_id"]

            
            if not store_ids.empty:
                store_ids.to_frame().to_parquet(out_file, index=False)
                print(f"    {store.upper():<15}: {len(store_ids):>3} IDs -> {out_file}")
                logger.info(f"Saved {len(store_ids)} IDs to {store} store file")
            else:
                # Create empty file for consistency
                pd.DataFrame(columns=["bundle_id"]).to_parquet(out_file, index=False)
                print(f"    {store.upper():<15}: No IDs")
                logger.debug(f"Created empty file for {store} store")

        print(f" All routing completed successfully!")
        logger.success("All routing completed successfully")
        return self.ROUTED_PATH