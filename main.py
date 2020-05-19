import asyncio
import json
import re
import datetime
from timeit import default_timer as timer

import aiohttp
import dataset
import numpy as np

ITEM_LIST_API = 'https://rsbuddy.com/exchange/summary.json'
ITEM_PRICE_API = 'https://services.runescape.com/m=itemdb_oldschool/api/graph/%s.json'
ITEM_DATA_URL = 'http://services.runescape.com/m=itemdb_oldschool/viewitem?obj=%s'

def parse_item_prices(html):
    prices = re.findall("(?<=average180\.push\(\[new Date\(\'(\d{4}\/\d{2}\/\d{2})\'\), )(\d+)", html)
    return [ (int(datetime.datetime.strptime(date, '%Y/%m/%d').timestamp()), price) for date, price in prices ]

def parse_item_volumes(html):
    volumes = re.findall("(?<=trade180\.push\(\[new Date\(\'(\d{4}\/\d{2}\/\d{2})\'\), )(\d+)", html)
    return [ (int(datetime.datetime.strptime(date, '%Y/%m/%d').timestamp()), vol) for date, vol in volumes ]

# Return 180 daily item prices, starting from today, in form (timestamp, price, volume)
async def fetch_item_data(session, url, item_id):
    raw_html = await fetch(session, url)
    prices = parse_item_prices(raw_html)
    # For now, discard timestamps -- should be equivalent to prices anyways
    volumes = [ vol for _, vol in parse_item_volumes(raw_html) ]

    data = [ (price[0], price[1], vol) for price, vol in zip(prices, volumes) ]
    return (item_id, data)

async def fetch(session, url):
    async with session.get(url) as resp:
        resp.raise_for_status()
        print(f'Status: {resp.status}')
        return await resp.text()

async def get_item_list(session):
    items = await fetch(session, ITEM_LIST_API)
    items = list(json.loads(items).keys())
    return items

async def fetch_bucket(session, bucket):
    while True:
        try:
            data = await asyncio.gather(*[ fetch_item_data(session, ITEM_DATA_URL % item, item) for 
                    item in bucket ])

            # Hacky way of detecting that ge site is ratelimiting us
            assert [] not in [ result for _, result in data ]
            return data
        except aiohttp.ClientResponseError as e:
            print('Request failed: e.message')
            print('Trying again in 1 minute..')
            await asyncio.sleep(30)
        except AssertionError:
            print('Rate limited. Waiting for 1 minute.')
            await asyncio.sleep(30)

async def main():
    bucket_size = 5
    sleep_time_mean = 10

    database = dataset.connect('sqlite:///osrs_prices.db')

    async with aiohttp.ClientSession(requote_redirect_url=False, raise_for_status=True) as session:
        items = await get_item_list(session)
        buckets = [ items[i:i+bucket_size] for i in range(0, len(items) - len(items)%bucket_size, bucket_size) ]
        if len(items) % bucket_size != 0:
            buckets.append(items[-(len(items)%bucket_size):])

        print(f'Fetched item list. Total items: {len(items)}')

        for bucket_no, bucket in enumerate(buckets):
            start = timer()
            bucket_data = await fetch_bucket(session, bucket)
            elapsed = timer() - start
            est_time_left = (len(buckets) - bucket_no) * ((elapsed) + sleep_time_mean)
            print(f'Fetched bucket {bucket_no}/{len(buckets)} in {elapsed*1000:.2f} ms. '
                f'Est time left: {est_time_left/60:.2f} min')

            for item_id, data in bucket_data:
                [ database[item_id].insert({'timestamp': ts, 'price': price, 'vol': vol}) for ts, price, vol in data ]

            print(f'Wrote bucket {bucket_no} to database.')
            await asyncio.sleep(np.random.normal(sleep_time_mean, 2))

if __name__ == '__main__':
    asyncio.run(main())
