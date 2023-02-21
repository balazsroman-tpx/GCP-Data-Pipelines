import asyncio

import aiohttp
import pandas as pd


async def get_data(url: str, headers: dict) -> dict:
    # This is an asynchronous function that uses aiohttp to make an HTTP GET request to a URL with the given headers.
    # It returns the response data as a JSON object.
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            data = await response.json()
            return data


async def get_data_for_page_range(
    url: str, start_page: int, end_page: int, headers: dict, key: str
) -> pd.DataFrame:
    # This is an asynchronous function that gets data from a range of pages using the get_data() function.
    # It creates a list of tasks for the given page range and uses asyncio.gather() to execute them in parallel.
    # It uses the key to get the required values from the response data and returns it as a list of dictionaries.
    # It uses the list of dictionaries to create a Pandas DataFrame.
    tasks = []
    # async with aiohttp.ClientSession() as session:
    for i in range(start_page, end_page + 1):
        this_url = f"{url}{i}"
        task = asyncio.ensure_future(get_data(this_url, headers))
        tasks.append(task)
    results = await asyncio.gather(*tasks)

    data = [item for result in results for item in result[key]]
    df = pd.DataFrame(data)
    return df


async def get_all_data(
    url: str, headers: dict, pages: int, key: str, batch_size: int = 10
) -> pd.DataFrame:
    # This is an asynchronous function that gets data from all pages in batches using the get_data_for_page_range() function.
    # It divides the page range into batches of batch_size pages and gets data for each batch in parallel using asyncio.gather().
    # It concatenates the data from each batch into a single Pandas DataFrame and returns it.
    dfs = []
    for start_page in range(1, pages + 1, batch_size):
        end_page = min(start_page + batch_size - 1, pages)
        print(f"Getting pages {start_page} to {end_page}")
        df = await get_data_for_page_range(url, start_page, end_page, headers, key)
        dfs.append(df)
    return pd.concat(dfs)
