import pymongo
from pymongo import MongoClient
import urllib.parse

from pymongo.synchronous.database import Database

username = urllib.parse.quote_plus("AdminBaxter")
password = urllib.parse.quote_plus("jaybot123")


def connection():
    client = MongoClient('mongodb://%s:%s@137.184.72.86:27052/' % (username, password))
    collection = client.jaybot
    return collection


def get_database(db_name: str) -> Database:
    client = MongoClient('mongodb://%s:%s@137.184.72.86:27052/' % (username, password))
    db = client.get_database(db_name)
    return db

