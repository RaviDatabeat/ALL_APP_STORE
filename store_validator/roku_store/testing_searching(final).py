import pandas as pd
import asyncio
from asyncio import Semaphore
from ddgs import DDGS
import re
import time
import random

# --- Helper functions ---
def normalize(text: str) -> str:
    return re.sub(r'[^a-z0-9]', '', text.lower())

def extract_app_name_from_url(url: str) -> str:
    """Extract app name from Roku URL like '.../tunein'"""
    if pd.isna(url):
        return ""
    
    # Extract the last part of the URL path
    url = str(url).strip()
    if '/' in url:
        # Get the last part after the last slash
        app_name = url.rstrip('/').split('/')[-1]
        return app_name
    return url

def _search_ddg(app_name: str):
    """Blocking DDGS search; to be run in executor"""
    time.sleep(random.uniform(0.05, 0.2))  # polite delay
    with DDGS() as ddgs:
        results = [r for r in ddgs.text(f"{app_name}.com", max_results=5)]
    if not results:
        return None

    norm_query = normalize(app_name)
    for r in results:
        href = r.get("href", "")
        
        # Skip Wikipedia results
        if "wikipedia.org" in href:
            continue
        elif "channelstore.roku.com":
            continue
            
        domain = re.sub(r"^https?://(www\.)?", "", href).split("/")[0]
        if norm_query in normalize(domain):
            return href
    
    # fallback to first non-Wikipedia result
    non_wiki_results = [r for r in results if "wikipedia.org" not in r.get("href", "")]
    if non_wiki_results:
        return non_wiki_results[0].get("href", None)
    
    return None

async def get_developer_url(app_name: str, sem: Semaphore):
    """Async wrapper using a thread pool for the blocking DDGS search"""
    async with sem:
        print(f"Searching developer URL for: {app_name}")
        loop = asyncio.get_event_loop()
        url = await loop.run_in_executor(None, _search_ddg, app_name)
        print(f"Found URL for {app_name}: {url}")
        return app_name, url

# --- Main async function ---
async def main():
    input_file = r"roku_testing\roku_app_data_updated.csv"
    df = pd.read_csv(input_file) 

    # First, let's see what columns we actually have
    print("Columns in CSV file:", df.columns.tolist())
    
    # Check if we have a URL column (try common names)
    url_column = None
    for col in ['url', 'URL', 'link', 'website', 'domain']:
        if col in df.columns:
            url_column = col
            break
    
    if not url_column:
        print("Error: No URL column found. Available columns:", df.columns.tolist())
        return
    
    print(f"Using column '{url_column}' for extracting app names")
    
    # Extract app names from the URL column
    print("Extracting app names from URLs...")
    df["appName"] = df[url_column].apply(extract_app_name_from_url)
    
    # Show some samples
    print("Sample extracted app names:")
    for i, (url, app_name) in enumerate(zip(df[url_column].head(5), df["appName"].head(5))):
        print(f"  {url} -> {app_name}")
    
    # Filter out empty app names
    df = df[df["appName"] != ""]
    print(f"After filtering empty names: {len(df)} rows")
    
    # FILTER: Only process apps where appstore_developer_url is empty
    if 'appstore_developer_url' in df.columns:
        # Count how many already have developer URLs
        existing_urls_count = df['appstore_developer_url'].notna().sum()
        print(f"Apps with existing developer URLs: {existing_urls_count}")
        
        # Keep only rows where appstore_developer_url is empty/NaN
        df_to_process = df[df['appstore_developer_url'].isna()]
        print(f"Apps to process (without developer URLs): {len(df_to_process)}")
    else:
        print("No 'appstore_developer_url' column found, processing all apps")
        df_to_process = df

    # Deduplicate app names from the filtered DataFrame
    unique_app_names = df_to_process["appName"].dropna().unique()
    print(f"Total unique apps to search: {len(unique_app_names)}")

    sem = Semaphore(25)  # max 25 concurrent searches
    tasks = [get_developer_url(name, sem) for name in unique_app_names]

    results = await asyncio.gather(*tasks)

    # Convert results to DataFrame
    df_devs = pd.DataFrame(results, columns=["appName", "Developer_URL"])

    # Merge back to original DataFrame - update only empty developer URLs
    if 'appstore_developer_url' in df.columns:
        # Create a copy to avoid modifying the original during merge
        df_result = df.copy()
        
        # Update only the rows that were processed (where developer_url was empty)
        for app_name, dev_url in results:
            mask = (df_result['appName'] == app_name) & (df_result['appstore_developer_url'].isna())
            df_result.loc[mask, 'appstore_developer_url'] = dev_url
    else:
        # If no developer_url column exists, create it
        df_result = df.merge(df_devs, on="appName", how="left")
        df_result.rename(columns={'Developer_URL': 'appstore_developer_url'}, inplace=True)

    # Save to CSV
    output_file = r"roku_testing\roku_store_searching_apps.csv"
    df_result.to_csv(output_file, index=False)
    print(f"Saved {len(df_result)} apps with developer URLs to: {output_file}")

# --- Run the async main ---
if __name__ == "__main__":
    asyncio.run(main())