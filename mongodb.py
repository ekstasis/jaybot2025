from pymongo import MongoClient
import urllib.parse

username = urllib.parse.quote_plus("AdminBaxter")
password = urllib.parse.quote_plus("jaybot123")


def connection():
    client = MongoClient('mongodb://%s:%s@137.184.72.86:27052/' % (username, password))
    collection = client.jaybot
    return collection


def get_database(db_name):
    client = MongoClient('mongodb://%s:%s@137.184.72.86:27052/' % (username, password))
    return client[db_name]

