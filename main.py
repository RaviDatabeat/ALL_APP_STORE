# CORRECTED main.py
import asyncio
from pathlib import Path
import pandas as pd
from loguru import logger
from dataclasses import dataclass, field
from validation import BundleValidator
from store_validator.apple_store import AppleStoreConfig
from store_validator.amazon_store import AmazonStoreConfig
from store_validator.microsft_store import MicrosoftManager
from store_validator.gallaxy import GallaxyManager
from store_validator.roku import rokuManager
from store_validator.lgstore import lgstoreManager
from store_validator.Samsung_app_store import Samsung_app_store_Manager
from store_validator.zeasn import ZeasnManager
from store_validator.android_store import appstoreManager
from store_validator.vizio import VizioManager
import sys
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv


load_dotenv()
# SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT")
# GOOGLE_SHEET_ID = os.getenv("SHEET_ID")
# GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME")

@dataclass
class DirectoryConfig:
    logs_dir: Path = Path("logs")
    store_logs_dir: Path = Path("store_logs")
    output_dir: Path = Path("output")
    routed_dir: Path = Path("routed_ids")
    permanent_file: Path = Path("output/combined_permanent.parquet")
    temp_file: Path = Path("output/combined_temp.parquet")

    
    def __post_init__(self):
        """Create all required directories"""
        self.logs_dir.mkdir(exist_ok=True)
        self.store_logs_dir.mkdir(exist_ok=True)
        self.output_dir.mkdir(exist_ok=True)
        self.routed_dir.mkdir(exist_ok=True)

    def setup_logger(self):
        logger.remove()
        self.logs_dir.mkdir(exist_ok=True)  # FIXED: Changed self.log_dir to self.logs_dir
        self.log_file = self.logs_dir / "main.log"  # FIXED
        logger.add(
            self.log_file,
            retention="10 days",
            level="INFO",
            format="{time} {level} {file.name}:{function}:{line} - {message}",
            backtrace=True,
            diagnose=True,
            enqueue=True, 
        )
        logger.add(sys.stdout, level="INFO")
        logger.info(f"Logging started in file: {self.log_file}")

    def load_existing_developer_urls(self) -> dict:  # FIXED: Removed staticmethod, fixed indentation
        """Load existing bundle_id to developer_url mappings from merge file"""
        if self.permanent_file.exists():
            try:
                existing_df = pd.read_parquet(self.permanent_file)
                if not existing_df.empty and 'bundle_id' in existing_df.columns and 'developer_url' in existing_df.columns:
                    url_mapping = existing_df.set_index('bundle_id')['developer_url'].to_dict()
                    print(f"Loaded {len(url_mapping)} existing developer URLs from merge file")
                    logger.info(f"Loaded {len(url_mapping)} existing developer URLs from merge file")
                    return url_mapping
            except Exception as e:
                logger.error(f"Error reading existing merge file: {e}")
                print(f"Error reading existing merge file: {e}")
        
        print("No existing merge file found or empty, starting fresh")
        return {}

    def merge_outputs(self, merged_output_file: Path):
        """Merge all output parquet files into one combined file"""
        # FIXED: Removed incorrect parameter
        existing_urls = self.load_existing_developer_urls()
        
        result_frames = []
        files = list(self.output_dir.glob("*.parquet"))
        logger.info(f"Found Parquet files: {[f.name for f in files]}")

        for file in files:
            if file.name == merged_output_file.name:
                continue 
            try:
                df = pd.read_parquet(file)
                
                if df.empty:
                    continue
                    
                if 'bundle_id' not in df.columns:
                    continue
                    
                if 'developer_url' not in df.columns:
                    df['developer_url'] = ''
                
                # FIXED: Use the method correctly
                df['developer_url'] = df.apply(lambda row: self.get_developer_url(row, existing_urls), axis=1)
                
                df = df[['bundle_id', 'developer_url']]
                df["source_store"] = file.stem
                result_frames.append(df)
                
            except Exception as e:
                logger.error(f"Error reading {file}: {e}")

        if result_frames:
            combined = pd.concat(result_frames, ignore_index=True)
            
            if 'bundle_id' not in combined.columns:
                combined['bundle_id'] = ''
            if 'developer_url' not in combined.columns:
                combined['developer_url'] = ''
            
            combined = combined[['bundle_id', 'developer_url', 'source_store']]
            combined = combined.drop_duplicates(subset=['bundle_id'], keep='last')
            
            combined.to_parquet(merged_output_file, index=False)
            logger.success(f"Merged output saved to: {merged_output_file}")
            print(f"Final merged file contains {len(combined)} unique bundle IDs")
        else:
            logger.warning("No parquet files found for merging.")
            if not merged_output_file.exists():
                empty_df = pd.DataFrame(columns=['bundle_id', 'developer_url', 'source_store'])
                empty_df.to_parquet(merged_output_file, index=False)
                print("Created empty merged file with required columns")

    def get_developer_url(self, row, existing_urls):  # ADDED: Missing method
        """Helper method for merge_outputs"""
        bundle_id = row['bundle_id']
        new_url = row['developer_url']
        if bundle_id in existing_urls and existing_urls[bundle_id]:
            return existing_urls[bundle_id]
        return new_url
    

    async def validate_and_route_ids(self, df_or_path) -> Path:
        """Validate and route bundle IDs from Excel file or DataFrame"""
        validator = BundleValidator()
        
        # If input is path, let validator handle it
        if isinstance(df_or_path, (str, Path)):
            routed_dir = await validator.validate_and_route_ids(df_or_path)
        elif isinstance(df_or_path, pd.DataFrame):
            routed_dir = await validator.validate_and_route_ids(df_or_path)
        else:
            raise ValueError(f"Invalid input type: {type(df_or_path)}")
        
        return routed_dir


    async def main(self):
        if self.temp_file.exists():
            self.temp_file.unlink()
        logger.info("Deleted old combined_temp.parquet")
        print("Starting bundle ID validation and routing...")
        logger.info("Starting bundle ID validation and routing")

        store_files = ["apple", "android", "amazon", "microsoft", "gallaxy",
                   "samsung", "zeasn", "vizio", "roku", "lg"]

        for store_name in store_files:
            store_file = self.routed_dir / f"{store_name}.parquet"
            if store_file.exists():
                try:
                    store_file.unlink()
                    logger.info(f"Deleted old routed file: {store_file}")
                except Exception as e:
                    logger.error(f"Failed to delete {store_file}: {e}")
        
        # FIXED: Corrected method call
        from pathlib import Path
        excel_file = Path("test.xlsx")
        if not excel_file.exists():
            raise FileNotFoundError(f"Input Excel file not found: {excel_file}")

        # Use BundleValidator directly with Excel file path
        validator = BundleValidator()
        self.routed_dir = await validator.validate_and_route_ids(excel_file)
        print(f"Routing completed. Files saved to: {self.routed_dir}")
        logger.info(f"Routing completed. Files saved to: {self.routed_dir}")

        # Define store processors
        store_processors = {
            "apple": AppleStoreConfig(),  # FIXED: Changed from appstoreManager
            "android": appstoreManager(),
            "amazon": AmazonStoreConfig(),
            "microsoft": MicrosoftManager(),
            "gallaxy": GallaxyManager(),
            "samsung": Samsung_app_store_Manager(),
            "zeasn": ZeasnManager(),
            "vizio": VizioManager(),
            "roku": rokuManager(),
            "lg": lgstoreManager()
        }

        print("\nProcessing each app store:")
        logger.info("Starting store processing")
        
        for store_name, processor in store_processors.items():
            store_file = self.routed_dir / f"{store_name}.parquet"
            
            if store_file.exists():
                df = pd.read_parquet(store_file)
                if not df.empty:
                    print(f"    {store_name.upper():<15} - {len(df)} bundle IDs")
                    logger.info(f"Processing {store_name} with {len(df)} IDs")
                    
                    try:
                        await processor.process(store_file)
                        print(f"       Successfully processed {store_name}")
                        
                    except Exception as e:
                        error_msg = f"Error processing {store_name}: {e}"
                        print(f"       {error_msg}")
                        logger.error(error_msg)
                else:
                    print(f"    {store_name.upper():<15} - No bundle IDs")
                    logger.info(f"{store_name} has no bundle IDs to process")
            else:
                print(f"    {store_name.upper():<15} - File not found")
                logger.warning(f"{store_name} file not found at {store_file}")
        
        # print(f"LG file exists: {(self.routed_dir / 'lg.parquet').exists()}")
        # print(f"Roku file exists: {(self.routed_dir / 'roku.parquet').exists()}")
        
        print("\nMerging all outputs...")
        logger.info("Merging all outputs")
        self.merge_outputs(merged_output_file=self.temp_file)      
        self.merge_outputs(merged_output_file=self.permanent_file)  
        print("All processing completed successfully!")
        logger.success("All processing completed successfully")

async def main_wrapper():  # Wrapper function
    config = DirectoryConfig()
    config.setup_logger()
    await config.main()

if __name__ == "__main__":
    asyncio.run(main_wrapper())  # Use the wrapper