import asyncio

import aiohttp
import pandas as pd


async def get_data(url, headers):
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            data = await response.json()
            return data


async def get_data_for_page_range(url, start_page, end_page, headers, key):
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


async def get_all_data(url, headers, pages, key, batch_size=10):
    dfs = []
    for start_page in range(1, pages + 1, batch_size):
        end_page = min(start_page + batch_size - 1, pages)
        print(f"Getting pages {start_page} to {end_page}")
        df = await get_data_for_page_range(url, start_page, end_page, headers, key)
        dfs.append(df)
    return pd.concat(dfs)
