import requests,os
# import asyncio
import asyncio
import pandas as pd
from io import StringIO
import httpx
from loguru import logger
from asyncio import Semaphore
# import pyarrow.parquet as pq



semaphore = Semaphore(5)
headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/plain,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive"
    }

data_buffer = []
PARQUET_FILE = "final_all_stores_file_testing__.parquet"


async def append_data(data):
        """Append data efficiently using fastparquet.to_parquet()"""
        df = pd.concat(data)

        if not df.empty:
            logger.info('Writing data to file')
            if os.path.exists(PARQUET_FILE):
                print('path exists:')
                df.to_parquet(PARQUET_FILE, engine="fastparquet", append=True, index = False)
            else:
                print('path not exists:')
                df.to_parquet(PARQUET_FILE, engine="fastparquet", index = False)
        data_buffer.clear()
        return



async def lst_to_df():
    global data_buffer
    if len(data_buffer) >= 100:
        logger.info(f"Size of data : {len(data_buffer)}")
        await append_data(data_buffer)
        # data_buffer.clear()
        # logger.info("Data has been appended to file")
    return


async def combined_dfs_data(ads_df,data):
    global data_buffer
    df = pd.DataFrame([data])

    if not ads_df.empty:
        df_expanded = pd.concat([df] * len(ads_df), ignore_index=True)
        final_df = pd.concat([df_expanded, ads_df], axis=1)
    else:
        final_df = pd.concat([df, ads_df], axis=1)
    data_buffer.append(final_df)
        
    return ####final_df 


async def read_ads_txt_with_pandas(text,data):
    if not text:
        return None

    try:
        df = pd.read_csv(
            StringIO(text),
            # comment="#",
            header=None,
            names=["full_data"],       
            sep="|",      
            engine="python",
            skip_blank_lines=True
        )
        

        if df.empty:
            return None
        
        df["full_data"] = df["full_data"].astype(str).str.strip()
        df = df[df["full_data"] != ""]

        logger.info(f"Fetched data for url: {data.get('url')} - developer url: {data.get('developer_url')}")
        return await combined_dfs_data(df, data)

    except Exception as e:
        logger.error(f"Error reading ads.tx data for url: {data.get('url')} - developer url : {data.get('developer_url')}")
        return await combined_dfs_data(pd.DataFrame(columns=["full_data"]), data)



async def fetch_ads_txt(client, url, data):
    async with semaphore:
        try:
            r = await client.get(url, follow_redirects=True)

            if r.status_code == 200 and r.text:
                logger.info(f"Fetching data: {data.get('url')} | ads.txt content URL: {url}")
                return await read_ads_txt_with_pandas(r.text, data)
                
            logger.warning(f"Non-200 response or empty content | status={r.status_code} | url={url}")
            return await combined_dfs_data(pd.DataFrame(columns=["full_data"]), data)   #None

        except Exception as e:
            logger.error(f"Error fetching ads.txt | source_url={data.get('url')} | ads_url={url} | error={e}", exc_info=True)
            return None


async def process_each_id(data, client):
    if not isinstance(data, dict):
        logger.error(f"Invalid data passed to process_each_id: {type(data)} → {data}")
        return 
    
    dev_url = data.get("developer_url")
    if not dev_url:
        logger.info(f"No developer URL for bundle_id: {data.get('bundle_id')}")
        return

    try:
        url = dev_url.rstrip("/")
        urls = [f"{url}/ads.txt", f"{url}/app-ads.txt"]
        tasks = [fetch_ads_txt(client, u,data) for u in urls]
        res = await asyncio.gather(*tasks)
        return await lst_to_df()


    except Exception as e:
        logger.warning(f"ads.txt fetch failed for {data.get('bundle_id')}: {e}")
        print('errorr')
        return 


async def process_all_bundle_ids(data_list):
    async with httpx.AsyncClient(timeout=10) as client:
        tasks = [process_each_id(data, client) for data in data_list]
        await asyncio.gather(*tasks)
        # results = await asyncio.gather(*tasks)
    logger.info(f"ads.txt processing completed.")
    if data_buffer:
        await append_data(data_buffer)
    return

