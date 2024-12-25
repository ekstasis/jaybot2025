from datetime import datetime, timezone, timedelta
import urllib.parse

from pymongo import MongoClient


def mongo_client():
    username = urllib.parse.quote_plus("AdminBaxter")
    password = urllib.parse.quote_plus("jaybot123")
    return MongoClient('mongodb://%s:%s@137.184.72.86:27052/' % (username, password))


if __name__ == '__main__':
    db = mongo_client()["jaybot"]
    collection = db['CoinbaseMatches']

    # Connect to MongoDB
    client = MongoClient('mongodb://localhost:27017')  # Replace with your MongoDB connection string
    db = client['your_database']  # Replace with your database name


    product = 'XLM-USD'

    now = datetime.now(tz=timezone.utc)
    duration =  timedelta(minutes=1)
    end = now.replace(second=0, microsecond=0)
    start = end - duration

    # Print the calculated times for reference
    print("Previous minute end:", start)
    print("Previous minute end:", end)

    # Query the database for BTC-USD trades within the previous minute
    pipeline = [
        {
            '$match': {
                'product_id': product,
                'time': {
                    '$gte': start,
                    '$lt': end
                }
            }
        },
        # Step 2: Group trades by some identifier (e.g., sequence of trades in this time window)
        {
            '$group': {
                '_id': None,  # No need to group by a specific field; just group all documents
                'trade_ids': {'$push': '$trade_id'}  # Collect all trade_ids in this time range
            }
        },

        # Step 3: Sort trade_ids to make sure they are in order
        {
            '$addFields': {
                'sorted_trade_ids': {'$sortArray': {'input': '$trade_ids', 'sortBy': 1}}
                # Sort trade_ids in ascending order
            }
        },

        # Step 4: Find missing trade_ids using $setDifference
        {
            '$addFields': {
                'gaps_in_trade_ids': {
                    '$let': {
                        'vars': {
                            'range': {'$range': [{'$arrayElemAt': ['$sorted_trade_ids', 0]},
                                                 {'$add': [{'$arrayElemAt': ['$sorted_trade_ids', -1]}, 1]}]}
                        },
                        'in': {'$setDifference': ['$$range', '$sorted_trade_ids']}
                    }
                }
            }
        },
        # Step 5: Project only the gaps, not all the trade_ids
        {
            '$project': {
                '_id': 0,  # Remove the _id field
                'gaps_in_trade_ids': 1  # Only include the gaps_in_trade_ids field
            }
        }
    ]

    # Execute the aggregation
    results = collection.aggregate(pipeline)

    # Process results
    for result in results:
        print(result)
