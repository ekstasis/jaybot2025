import asyncio, json, websockets
from datetime import datetime
import mongodb as db

conn = db.connection()

URI = 'wss://ws-feed.exchange.coinbase.com'

channel = 'trades'
db_collection = 'CoinbaseMatches'

product_ids = ['XLM-USD']
product_ids += ['BTC-USD']
product_ids += ['ETH-USD']


def convert_iso_8601_time(json_response):
    """ Replaces the time string from Coinbase with a python datetime object.
        This is required by MongoDB Time Series """
    iso_8601_time_string = json_response['time']
    python_time = datetime.fromisoformat(iso_8601_time_string.replace('Z', '+00:00'))
    json_response['time'] = python_time


def write_match_to_database(json_response):
    """ No need to change the json except for the time before writing it to DB """
    convert_iso_8601_time(json_response)
    collection = conn[db_collection]
    collection.insert_one(json_response)


def process_response(json_response):
    if json_response['type'] == 'match':
        write_match_to_database(json_response)
    else:
        print(json_response)  # Will at least display the subscription method before writing trades to DB


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
                    json_response = json.loads(response)
                    process_response(json_response)
        except (websockets.exceptions.ConnectionClosedError, websockets.exceptions.ConnectionClosedOK):
            print('Connection closed, retrying..')
            await asyncio.sleep(1)


if __name__ == '__main__':
    try:
        asyncio.run(websocket_listener())
    except KeyboardInterrupt:
        print("Exiting WebSocket..")
