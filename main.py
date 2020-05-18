import asyncio
import json
from timeit import default_timer as timer

import aiohttp
import dataset

ITEM_LIST_API = 'https://rsbuddy.com/exchange/summary.json'
ITEM_PRICE_API = 'https://services.runescape.com/m=itemdb_oldschool/api/graph/%s.json'

# Return 180 daily item prices, starting from today
async def fetch_item_prices(session, url, item_id):
    prices = await fetch(session, url)
    prices = prices['daily']
    return (item_id, prices)

async def fetch(session, url):
    async with session.get(url) as resp:
        return json.loads(await resp.text())

async def get_item_list(session):
    items = await fetch(session, ITEM_LIST_API)
    items = list(items.keys())
    return items

'''
Basic idea:
    - Table for each item
    - Each table contains timestamp, price for daily return
'''

async def main():
    bucket_size = 10
    sleep_time = 10

    database = dataset.connect('sqlite:///osrs_prices.db')

    async with aiohttp.ClientSession() as session:
        items = await get_item_list(session)
        buckets = [ items[i:i+bucket_size] for i in range(0, len(items) - len(items)%bucket_size, bucket_size) ]
        if len(items) % bucket_size != 0:
            buckets.append(items[-(len(items)%bucket_size):])

        print(f'Fetched item list. Total items: {len(items)}')

        for bucket_no, bucket in enumerate(buckets):
            start = timer()
            bucket_data = await asyncio.gather(*[ fetch_item_prices(session, ITEM_PRICE_API % item, item) for 
                item in bucket ])
            elapsed = timer() - start
            est_time_left = (len(buckets) - bucket_no) * ((elapsed) + sleep_time)
            print(f'Fetched bucket {bucket_no}/{len(buckets)} in {elapsed*1000:.2f} ms. '
                f'Est time left: {est_time_left/60:.2f} min')

            for item_id, data in bucket_data:
                [ database[item_id].insert({'timestamp': ts, 'price': price}) for ts, price in data.items() ]

            print(f'Wrote bucket {bucket_no} to database.')
            await asyncio.sleep(sleep_time)

asyncio.run(main())
