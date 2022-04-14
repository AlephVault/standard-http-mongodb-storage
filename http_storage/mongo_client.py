import os
from urllib.parse import quote_plus
from pymongo import MongoClient


USER = os.environ['MONGODB_USER']
PASSWORD = os.environ['MONGODB_PASSWORD']
HOST = os.environ.get('MONGODB_HOST', 'localhost')
PORT = os.environ.get('MONGODB_PORT', '27017')
client = MongoClient("mongodb://%s:%s@%s:%s" % (quote_plus(USER), quote_plus(PASSWORD), HOST, PORT))