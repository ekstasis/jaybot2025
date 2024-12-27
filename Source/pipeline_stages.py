import datetime


def price_pct_change(start: datetime.datetime, end: datetime.datetime):
    time_match = match_time_interval_stage(start, end)
    pipeline = [time_match,
                {"$sort": {"time": 1}},
                {"$group": {
                    "_id": "$product_id",
                    "first_price": {"$first": "$price"},
                    "last_price": {"$last": "$price"},
                }},
                {"$addFields": {
                    "chg_pct": {
                        "$multiply": [100.0,
                                      {"$divide": [
                                          {"$subtract": ["$last_price", "$first_price"]}, "$first_price"]}
                                      ]
                    }
                ]
    return pipeline

def match_time_interval_stage(start: datetime.datetime, end: datetime.datetime):
    return {"$match": {
        "time": {
            "$gte": start, "$lt": end
        }
    }}