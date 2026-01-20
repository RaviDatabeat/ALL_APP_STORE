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

def _search_ddg(app_name: str):
    """Blocking DDGS search; to be run in executor"""
    time.sleep(random.uniform(0.05, 0.2))  # polite delay
    with DDGS() as ddgs:
        results = [r for r in ddgs.text(f"{app_name} developer", max_results=5)]
    if not results:
        return None

    norm_query = normalize(app_name)
    for r in results:
        href = r.get("href", "")
        domain = re.sub(r"^https?://(www\.)?", "", href).split("/")[0]
        if norm_query in normalize(domain):
            return href
    # fallback to first result
    return results[0].get("href", None)

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
    input_csv = r"C:\work\test_app\lg_all_apps.csv"
    df = pd.read_csv(input_csv)

    # Deduplicate app names
    unique_app_names = df["appName"].dropna().unique()
    print(f"Total unique apps: {len(unique_app_names)}")

    sem = Semaphore(25)  # max 25 concurrent searches
    tasks = [get_developer_url(name, sem) for name in unique_app_names]

    results = await asyncio.gather(*tasks)

    # Convert results to DataFrame
    df_devs = pd.DataFrame(results, columns=["appName", "Developer_URL"])

    # Merge back to original DataFrame
    df = df.merge(df_devs, on="appName", how="left")

    # Save to CSV
    output_csv = r"C:\work\All_apps\store_validators\lg_all_apps_with_developer_urls_async.csv"
    df.to_csv(output_csv, index=False)
    print(f"Saved {len(df)} apps with developer URLs to: {output_csv}")

# --- Run the async main ---
if __name__ == "__main__":
    asyncio.run(main())
