import logging
from datetime import datetime, timedelta, timezone
from apscheduler.schedulers.background import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from pymongo.synchronous.collection import Collection

import config
from mongodb import get_database


def print_price_change(collection: Collection, interval: timedelta):
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
    print(f'\n{start_str} -> {end_str}\n==============')
    price_changes_per_product = list(collection.aggregate(pipeline))
    if not price_changes_per_product:
        print("No Trades")
        return

    for product in price_changes_per_product:
        dec_chg = product['chg_pct'].to_decimal()
        print(f"{product['_id']}:  {float(dec_chg):.2f}%")


def minutely_report(collection: Collection):
    print_price_change(collection=collection, interval=timedelta(minutes=1))


def hourly_report(collection: Collection):
    print_price_change(collection=collection, interval=timedelta(hours=1))

if __name__ == '__main__':

    logging.basicConfig()
    logging.getLogger('apscheduler').setLevel(logging.DEBUG)

    db = get_database('jaybot')
    trades = db.get_collection(config.collection_name)

    print(f'\nUsing collection: \"{config.collection_name}\"\n')

    scheduler = BlockingScheduler()
    scheduler.add_job(minutely_report, CronTrigger(minute="0-59"), args=[trades])
    scheduler.add_job(hourly_report, CronTrigger(minute="0"), args=[trades])
    scheduler.print_jobs()
    scheduler.start()
