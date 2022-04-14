import typing as t
from bson import ObjectId
from flask import Flask, current_app as app, request
from flask.json import JSONDecoder, JSONEncoder


class MongoDBEnhancedEncoder(JSONEncoder):
    """
    This is an enhancement over a Flask's JSONEncoder but with
    adding the encoding of an ObjectId to string.
    """

    def default(self, o: t.Any) -> t.Any:
        if isinstance(o, ObjectId):
            return str(o)
        return super().default(o)


class StorageApp(Flask):
    """
    A Standard HTTP Storage app. Among other things, it provides
    a way to interact with MongoDB.
    """

    json_encoder = MongoDBEnhancedEncoder
    auth_db = None
    auth_table = None
