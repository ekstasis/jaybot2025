from mongodb import get_database
from datetime import datetime


test_collection = get_database()['jb_test']

start_time = datetime.fromisoformat("2024-12-26T21:45:00.000+00:00")
end_time = datetime.fromisoformat("2024-12-26T21:46:00.000+00:00")

match_stage_product = {"$match": {"product_id": "XLM-USD"}}

match_stage_time = {
    "$match": {
        "timestamp": {
            "$gte": start_time,  # start of the minute
            "$lt": end_time  # end of the minute
        }
    }
}

group_stage = {
    "$group": {
        "_id": "NULL",
        "count": {"$count": {}}
    }
}
pipeline = [ match_stage_time, group_stage]
# pipeline = [
#     {
#         "$match": {
#             "product_id": "XLM-USD"
#         }
#     }
# ]
results = test_collection.aggregate(pipeline)

print()

for result in results:
    print(result)
