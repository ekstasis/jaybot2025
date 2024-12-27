import asyncio
import json
import websockets
import time
from datetime import datetime
from bson.decimal128 import Decimal128
from mongodb import get_database

""" Reference for "match" JSON: 
    https://docs.cdp.coinbase.com/exchange/docs/websocket-channels#match  
"""


URI = 'wss://ws-feed.exchange.coinbase.com'
channel = 'matches'

collection_name = 'CoinbaseMatches'
# collection_name = 'jb_test'

db_collection = get_database('jaybot')[collection_name]

product_ids = ['XLM-USD']
product_ids += ['BTC-USD']
product_ids += ['ETH-USD']

print_times = True  # print the timestamp of each trade written to console


def convert_iso_8601_time(json_response):
    """ Replaces the time string from Coinbase with a python datetime object.
        This is required by MongoDB Time Series """
    iso_8601_time_string = json_response['time']
    python_time = datetime.fromisoformat(iso_8601_time_string.replace('Z', '+00:00'))
    json_response['time'] = python_time


def prepare_match(match):
    """ Change text fields to data/numeric so mongodb can operate on them """
    # date
    iso_8601_time_string = match['time']
    python_time = datetime.fromisoformat(iso_8601_time_string.replace('Z', '+00:00'))
    match['time'] = python_time

    # price & size
    match['size'] = Decimal128(match['size'])
    match['price'] = Decimal128(match['price'])


def write_match_to_database(match: dict, print_time: bool):
    prepare_match(match)
    db_collection.insert_one(match)
    if print_time:
        print(match['time'])


def process_response(response):
    global print_times
    if response['type'] == 'match':
        write_match_to_database(response, print_time=print_times)
    else:
        print(response)  # Will at least display the subscription method before writing trades to DB


async def websocket_listener():
    subscribe_message = json.dumps({
        'type': 'subscribe',
        'channels': [{'name': channel, 'product_ids': product_ids}]
    })

    while True:
        try:
            async with websockets.connect(URI, ping_interval=None) as websocket:
                await websocket.send(subscribe_message)
                while True:
                    response = await websocket.recv()
                    process_response(json.loads(response))
        except (websockets.exceptions.ConnectionClosedError, websockets.exceptions.ConnectionClosedOK):
            print('Connection closed, retrying..')
            await asyncio.sleep(1)


if __name__ == '__main__':

    delay = 5
    print(f'\nWill being WRITING to collection: \"{collection_name}\" in {delay} seconds...\n')
    time.sleep(delay)

    print_times = False

    try:
        asyncio.run(websocket_listener())
    except KeyboardInterrupt:
        print("Exiting WebSocket..")
