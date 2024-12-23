import asyncio, json, websockets
import mongodb as db

conn = db.connection()

URI = 'wss://ws-feed.exchange.coinbase.com'

channel = 'matches'
product_ids = ['XLM-USD'], 'ETH-USD', 'BTC-USD']

def coin_schema(_type, trade_id, maker_order_id, taker_order_id, size, price, product_id, sequence, time):
    schema = {
        "type": _type,
        "trade_id": trade_id,
        "price": price,
        "maker_order_id": maker_order_id,
        "taker_order_id": taker_order_id,
        "size": size,
        "product_id": product_id,
        "sequence": sequence,
        "time": time,
        "can_delete": True
        }
    return schema

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
                    if json_response['type'] == 'match':
                        _type = json_response['side']
                        trade_id = json_response['trade_id']
                        price = json_response['price']
                        maker_order_id = json_response['maker_order_id']
                        taker_order_id = json_response['taker_order_id']
                        #side = json_response['side']
                        size = json_response['size']
                        product_id = json_response['product_id']
                        sequence = json_response['sequence']
                        time = json_response['time']
                        #print(product_id)
                        if product_id == 'XLM-USD':
                            matches = conn['XLM']
                        elif product_id == 'BTC-USD':
                            matches = conn['BTC']
                        elif product_id == 'ETH-USD':
                            matches = conn['ETH']
                        matches.insert_one(coin_schema(_type,trade_id,maker_order_id,taker_order_id,size,price,product_id,sequence,time))
                    #print(json_response)

        except (websockets.exceptions.ConnectionClosedError, websockets.exceptions.ConnectionClosedOK):
            print('Connection closed, retrying..')
            await asyncio.sleep(1)


if __name__ == '__main__':
    try:
        asyncio.run(websocket_listener())
    except KeyboardInterrupt:
        print("Exiting WebSocket..")
