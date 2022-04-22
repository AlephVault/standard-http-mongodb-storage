import functools
from datetime import datetime
from bson import ObjectId
from flask import Flask, make_response, request
from flask.json import jsonify
from .core.json import MongoDBEnhancedEncoder
from .core.validation import MongoDBEnhancedValidator
from .engine.client import CLIENT
from .engine.schemas import *


class ImproperlyConfiguredError(Exception):
    """
    Raised when the storage app is misconfigured.
    """


class StorageApp(Flask):
    """
    A Standard HTTP Storage app. Among other things, it provides
    a way to interact with MongoDB.
    """

    json_encoder = MongoDBEnhancedEncoder
    validator_class = MongoDBEnhancedValidator
    timestamp_with_splitseconds = False

    def __init__(self, resources, validator_class=None, *args, **kwargs):
        """
        Checks a validator_class is properly configured, as well as the auth_db / auth_table.

        :param resources: The schema being used for this app.
        :param args: The flask-expected positional arguments.
        :param kwargs: The flask-expected keyword arguments.
        """

        # First, set the non-None arguments, overriding the per-class setup.
        self._validator_class = validator_class or self.validator_class

        # Also, set everything up to keep schema decorators.
        self._schema_decorators = {}

        # Then, validate them (regardless being instance or class arguments).
        # Once the schema is validated, keep the normalized document for future
        # uses (e.g. extract the auth db/collection from it, and later extract
        # all the resources' metadata from it).
        if not (isinstance(self._validator_class, type) and issubclass(self._validator_class, MongoDBEnhancedValidator)):
            raise ImproperlyConfiguredError("Wrong or missing validator class")
        validator = self._validator_class("http_storage.schemas.settings")
        if not validator.validate(resources):
            raise ImproperlyConfiguredError(f"Validation errors on resource schema: {validator.errors}")
        self._resources = validator.document

        super().__init__(*args, **kwargs)

    def _bearer_required(self, f):
        """
        Requires a valid "Authorization: Bearer xxxxx..." header.
        :param f: The function to invoke.
        :return: The decorated function.
        """

        def wrapper(*args, **kwargs):
            # Get the auth settings.
            auth_db = self._resources['auth']['db']
            auth_table = self._resources['auth']['collection']
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
            token = CLIENT[auth_db][auth_table].find_one({'_id': ObjectId(token),
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
