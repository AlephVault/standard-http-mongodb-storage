import functools
import os
import typing as t
from datetime import date, datetime
from urllib.parse import quote_plus
from bson import ObjectId
from cerberus import Validator, TypeDefinition
from pymongo import MongoClient
from flask import Flask, current_app as app, make_response, request
from flask.json import JSONEncoder, jsonify


class ImproperlyConfiguredError(Exception):
    """
    Raised when the storage app is misconfigured.
    """


_DATETIME_FORMATS = [
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
]
_DATE_FORMAT = "%Y-%m-%d"


USER = os.environ['MONGODB_USER']
PASSWORD = os.environ['MONGODB_PASSWORD']
HOST = os.environ.get('MONGODB_HOST', 'localhost')
PORT = os.environ.get('MONGODB_PORT', '27017')
client = MongoClient("mongodb://%s:%s@%s:%s" % (quote_plus(USER), quote_plus(PASSWORD), HOST, PORT))


class MongoDBEnhancedEncoder(JSONEncoder):
    """
    This is an enhancement over a Flask's JSONEncoder but with
    adding the encoding of an ObjectId to string, and custom
    encodings for the date and datetime types.
    """

    def default(self, o: t.Any) -> t.Any:
        if isinstance(o, ObjectId):
            return str(o)
        elif isinstance(o, datetime):
            use_splitseconds = getattr(app, 'timestamp_with_splitseconds', False)
            return o.strftime(_DATETIME_FORMATS[0] if use_splitseconds else _DATETIME_FORMATS[2])
        elif isinstance(o, date):
            return o.strftime(_DATE_FORMAT)
        return super().default(o)


class MongoDBEnhancedValidator(Validator):
    """
    This validator adds the following:
    - Registering types: objectid.
    - Default coercion of objectid using ObjectId.
    - Default coercion of date and datetime using custom formats.
    """

    types_mapping = {
        **Validator.types_mapping,
        "objectid": TypeDefinition("objectid", (ObjectId,), ()),
    }

    def _normalize_coerce_str2date(self, value):
        """
        Coerces a date from a string in a %Y-%m-%d format.
        :param value: The string value to coerce.
        :return: The coerced date.
        """

        return datetime.strptime(value, _DATE_FORMAT).date()

    def _normalize_coerce_str2datetime(self, value):
        """
        Coerces a datetime from a string in one of the available formats.
        :param value: The string value to coerce.
        :return: The coerced date.
        """

        for format in _DATETIME_FORMATS:
            try:
                return datetime.strptime(value, format)
            except ValueError:
                continue
        raise ValueError(f"time data '{value}' does not match any of the available formats")

    @classmethod
    def apply_default_coercers(cls, schema, tracked=None):
        """
        In-place modifies a schema to add the default coercers to the input
        documents before validation. This method should be called only once
        per schema.
        :param schema: The schema to in-place modify and add the coercers.
        :param tracked: The already-tracked levels for this schema.
        """

        # Circular dependencies are ignored - they are already treated.
        if tracked is None:
            tracked = set()
        schema_id = id(schema)
        if schema_id in tracked:
            return

        if 'coerce' not in schema:
            type_ = schema.get('type')
            if type_ == "objectid":
                schema['coerce'] = ObjectId
            elif type_ == "date":
                schema['coerce'] = 'str2date'
            elif type_ == "datetime":
                schema['coerce'] = 'str2datetime'
        # We iterate over all the existing dictionaries to repeat this pattern.
        # For this we also track the current schema, to avoid circular dependencies.
        tracked.add(schema_id)
        for sub_schema in schema.values():
            if isinstance(sub_schema, dict):
                cls.apply_default_coercers(sub_schema, tracked)
        tracked.remove(schema_id)


class StorageApp(Flask):
    """
    A Standard HTTP Storage app. Among other things, it provides
    a way to interact with MongoDB.
    """

    json_encoder = MongoDBEnhancedEncoder
    validator_class = MongoDBEnhancedValidator
    timestamp_with_splitseconds = False
    auth_db = None
    auth_table = None

    def __init__(self, auth_db=None, auth_table=None, validator_class=None, *args, **kwargs):
        """
        Checks a validator_class is properly configured, as well as the auth_db / auth_table.

        :param auth_db: The db, in mongo server, used for authentication.
        :param auth_table: The table, in mongo server, used for authentication.
        :param args: The flask-expected positional arguments.
        :param kwargs: The flask-expected keyword arguments.
        """

        # First, set the non-None arguments, overriding the per-class setup.
        self._validator_class = validator_class or self.validator_class
        self._auth_db = auth_db or self.auth_db
        self._auth_table = auth_table or self.auth_table

        # Also, set everything up to keep schema decorators.
        self._schema_decorators = {}

        # Then, validate them (regardless being instance or class arguments).
        if not (isinstance(self._validator_class, type) and issubclass(self._validator_class, MongoDBEnhancedValidator)):
            raise ImproperlyConfiguredError("Wrong or missing validator class")
        if not (self._auth_db and self._auth_table):
            raise ImproperlyConfiguredError("Wrong or missing auth_db / auth_table settings")

        super().__init__(*args, **kwargs)

    def _bearer_required(self, f):
        """
        Requires a valid "Authorization: Bearer xxxxx..." header.
        :param f: The function to invoke.
        :return: The decorated function.
        """

        def wrapper(*args, **kwargs):
            # Get the auth settings.
            auth_db = self._auth_db
            auth_table = self._auth_table
            # Get the header. It must be "bearer {token}".
            authorization = request.headers.get('Authorization')
            if not authorization:
                return make_response(jsonify({'code': 'authorization:missing-header'}), 401)
            # Split it, and expect it to be "bearer".
            try:
                scheme, token = authorization.split(' ')
                if scheme.lower() != 'bearer':
                    return make_response(jsonify({'code': 'authorization:bad-scheme'}), 400)
            except ValueError:
                return make_response(jsonify({'code': 'authorization:syntax-error'}), 400)
            # Check the token.
            token = client[auth_db][auth_table].find_one({'_id': ObjectId(token),
                                                          'valid_until': {"$not": {"$lt": datetime.now()}}})
            if not token:
                return make_response(jsonify({'code': 'authorization:not-found'}), 401)
            # If the validation passed, then we invoke the decorated function.
            return f(*args, **kwargs)
        return wrapper

    def _schema_required(self, schema):
        """
        Returns a decorator, with a given schema, that wraps a function
        that requires a json body matching the given schema in order to
        proceed.
        :return: A decorator.
        """

        MongoDBEnhancedValidator.apply_default_coercers(schema)
        # Get the id of the schema.
        schema_id = id(schema)
        # Attempt to get the already-existing schema decorator.
        schema_decorator = self._schema_decorators.get(schema_id)
        if schema_decorator:
            return schema_decorator

        # Define and store the decorator, and return it.
        def wrapper(f):
            validator = self._validator_class(schema)

            # This is the function that will run the logic.
            @functools.wraps(f)
            def wrapped(*args, **kwargs):
                # First, we extract the JSON body.
                try:
                    if not request.is_json:
                        pass
                    data = request.json
                except Exception as e:
                    return make_response(jsonify({'code': 'format:unexpected'}), 400)

                # Then, we validate the JSON body against the
                # required schema, and invoke the decorated
                # function with the normalized valid object.
                if validator.validate(data):
                    return f(validator.document, *args, **kwargs)
                else:
                    return make_response(jsonify({'code': 'schema:invalid', 'errors': validator.errors}), 400)
            return wrapped

        self._schema_decorators[schema_id] = wrapper
        return wrapper
