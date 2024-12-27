import time
from datetime import datetime, timedelta, timezone

from mongodb import jaybot_db

test_collection = jaybot_db()['jb_test']


def print_price_change(collection):
    """Prints the % change in price over the last minute for each product """

    # Calculate edges of previous minute
    now = datetime.now(tz=timezone.utc)
    end_time = now.replace(second=0, microsecond=0)  # Start of the current minute
    start_time = end_time - timedelta(minutes=1)  # Start of the previous minute

    # filter, sort, group, calculate
    pipeline = [
        {"$match": {"time": {"$gte": start_time, "$lt": end_time}}},
        {"$sort": {"time": 1}},
        {"$group": {
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

    print('=====================================')
    price_changes_per_product = list(collection.aggregate(pipeline))
    if not price_changes_per_product:
        print("No Trades")
        return

    for product in price_changes_per_product:
        dec_chg = product['chg_pct'].to_decimal()
        shrt_end = end_time.strftime('%H:%M')
        print(f"{shrt_end} | {product['_id']} 1-minute price change:  {float(dec_chg):.2f}%")


while True:
    try:
        print_price_change(test_collection)

        # Sleep briefly before checking again
        time.sleep(60)  # Adjust interval as needed
    except Exception as e:
        print(f"Error: {e}")
