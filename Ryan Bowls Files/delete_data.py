from pymongo import MongoClient
import urllib.parse

username = urllib.parse.quote_plus("AdminBaxter")
password = urllib.parse.quote_plus("jaybot123")

client = MongoClient('mongodb://%s:%s@137.184.72.86:27052/' % (username, password))
collection = client.jaybot
matches1 = collection.BTC
matches2 = collection.ETH
matches3 = collection.XLM



matches1.delete_many({'can_delete': True})
matches2.delete_many({'can_delete': True})
matches3.delete_many({'can_delete': True})
