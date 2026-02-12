import asyncio
import os
from pathlib import Path
from curl_cffi.requests import AsyncSession
from asyncio import Semaphore
import pandas as pd
from lxml import html
from typing import Dict, Optional


from loguru import logger

current_path = Path.cwd()

# Configure logger
logger.remove()  # Remove the default logger
# logger.add(sys.stdout, level="DEBUG", format="{time} {level} {file.name}:{function}:{line} - {message}", backtrace=True, diagnose=True)
logger.add(
    current_path / "roku_app.log",
    retention="10 days",
    level="INFO",
    format="{time} {level} {file.name}:{function}:{line} - {message}",
    backtrace=True,
    diagnose=True,
)

logger.info(current_path)

df = pd.read_csv(r"Roku_testing\NEW_roku_store_apps.csv")



semaphore = Semaphore(5)

urls = [
    "https://channelstore.roku.com/details/6b51d431df5d7f141cbececcf79edf3d:f4f106825aa59985b092fc699f825ca0/netflix",
    "https://channelstore.roku.com/details/8527a891e224136950ff32ca212b45bc:efdff7389cbb4a5d24c7fee438b4788f/mlb",
    "https://channelstore.roku.com/details/501a4e61aa4f7737df0305124a39119b:9f0f785535ad319cd05f98e8af0171c6/youtube",
]
urls = df["loc"].unique()

output_data_list = []
batch = 100


def extract_appstore_meta_tags(content) -> Dict[str, Optional[str]]:
    # with open(html_path, "r", encoding="utf-8") as f:
    #     content = f.read()

    tree = html.fromstring(content)

    def get_meta_content(name: str) -> Optional[str]:
        result = tree.xpath(f'//meta[@name="{name}"]/@content')
        return result[0] if result else None

    return {
        "appstore_store_id": get_meta_content("appstore:store_id"),
        "appstore_bundle_id": get_meta_content("appstore:bundle_id"),
        "appstore_developer_url": get_meta_content("appstore:developer_url"),
    }


def append_to_parquet(data, file_path):
    """Append data efficiently using fastparquet.to_parquet()"""

    df = pd.DataFrame(data)

    df = df.astype("string[pyarrow]").fillna("")

    # logger.info(df.dtypes)

    if not df.empty:
        if os.path.exists(file_path):
            df.to_parquet(file_path, engine="fastparquet", append=True, index=False)
        else:
            df.to_parquet(file_path, engine="fastparquet", index=False)


async def fetch_with_retry(
    session: AsyncSession, url: str, retries: int = 3, timeout: int = 30
) -> tuple[str, int | None]:
    attempt = 0
    global output_data_list
    while attempt < retries:
        async with semaphore:
            try:
                response = await session.get(
                    url, timeout=timeout, impersonate="chrome", allow_redirects=True
                )
                res_status_code = response.status_code
                logger.info(f"[{res_status_code}] {url}")
                response.raise_for_status()

                meta_data = extract_appstore_meta_tags(response.text)
                meta_data["url"] = url
                meta_data["status_code"] = res_status_code

                output_data_list.append(meta_data)

                if len(output_data_list) >= batch:
                    append_to_parquet(
                        output_data_list, "rokuAll_new.parquet"
                    )
                    output_data_list = []

                del response
                await asyncio.sleep(2)
                return {"url": url, "res_status_code": res_status_code}

            except Exception as e:
                logger.error(f"Retry {attempt + 1} failed for {url}: {e}")
                attempt += 1
                await asyncio.sleep(15)
    return {"url": url, "res_status_code": "N/A"}


async def main():
    async with AsyncSession(impersonate="chrome", allow_redirects=True) as session:
        tasks = [fetch_with_retry(session, url) for url in urls]
        results = await asyncio.gather(*tasks)

        append_to_parquet(output_data_list, "rokuAll_new.parquet")

        # Convert to dict: {url: status_code}
        final_df = pd.DataFrame(results)

        return final_df


if __name__ == "__main__":
    result_df = asyncio.run(main())

    result_df.to_csv("rokuAll_new.csv", index=False)
