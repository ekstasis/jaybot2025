import time
from datetime import datetime, timedelta, timezone
import schedule

from mongodb import get_database
from config import collection_name


def print_price_change(collection: dict, interval: timedelta):
    """Prints the % change in price over the last minute for each product
    """
    # Calculate edges of previous minute
    now = datetime.now(tz=timezone.utc)
    end_time = now.replace(second=0, microsecond=0)
    start_time = end_time - interval

    # filter, sort, group, calculate
    pipeline = [
        {"$match": {"time": {"$gte": start_time, "$lt": end_time}}},
        {"$sort": {"time": 1}},
        {"$group": {  # returns one document for each product
            "_id": "$product_id",
            "first_price": {"$first": "$price"},
            "last_price": {"$last": "$price"},
        }},
        {"$addFields": {
            # Percentage change in price = 100.0 * (last_price - first_price) / first_price
            "chg_pct": {
                "$multiply": [100.0, {
                    "$divide": [
                        {"$subtract": ["$last_price", "$first_price"]},
                        "$first_price"
                    ]
                }
                              ]
            }
        }}
    ]

    start_str = start_time.strftime('%H:%M')
    end_str = end_time.strftime('%H:%M')
    print(f'\n=== {start_str} -> {end_str} ===')
    price_changes_per_product = list(collection.aggregate(pipeline))
    if not price_changes_per_product:
        print("No Trades")
        return

    for product in price_changes_per_product:
        dec_chg = product['chg_pct'].to_decimal()
        print(f"{product['_id']} 1-minute price change:  {float(dec_chg):.2f}%")


if __name__ == '__main__':
    """
    Every 60 seconds, report previous minute's price change for each product
    """
    trades = get_database('jaybot')[collection_name]

    one_minute = timedelta(minutes=1)
    one_hour = timedelta(hours=1)

    # schedule.every().minute.do(print_price_change, collection=trades, interval=one_minute)
    schedule.every().hour.do(print_price_change, collection=trades, interval=one_hour)

    print(f'\n\nUsing collection: \"{collection_name}\"')
    seconds_till = schedule.idle_seconds()
    if seconds_till < 60: # ?  f'{seconds_till:0f}' : f'{seconds_till / 60:1f}'
        print(f"Next job in {seconds_till:.0f} seconds")
    else:
        print(f"Next job in {seconds_till / 60:.1f} minutes")

    print(schedule.next_run())

    while True:
        schedule.run_pending()
        time.sleep(60)