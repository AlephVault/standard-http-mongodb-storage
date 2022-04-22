import functools
from datetime import datetime
from typing import Callable
from bson import ObjectId
from flask import Flask, make_response, request
from flask.json import jsonify
from pymongo import ASCENDING, DESCENDING
from .core.json import MongoDBEnhancedEncoder
from .core.validation import MongoDBEnhancedValidator
from .engine.client import CLIENT
from .engine.schemas import *


class ImproperlyConfiguredError(Exception):
    """
    Raised when the storage app is misconfigured.
    """


NOT_FOUND = {"code": "not-found"}, 404


class StorageApp(Flask):
    """
    A Standard HTTP Storage app. Among other things, it provides
    a way to interact with MongoDB.
    """

    json_encoder: type = MongoDBEnhancedEncoder
    validator_class: type = MongoDBEnhancedValidator
    timestamp_with_splitseconds: bool = False

    def __init__(self, resources: dict, validator_class: type = None, *args, **kwargs):
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

        # After everything is initialized, the endpoints must be registered.
        # Those are standard resource endpoints.
        self._register_endpoints()

    def _bearer_required(self, f: Callable):
        """
        Requires a valid "Authorization: Bearer xxxxx..." header.
        :param f: The function to invoke.
        :return: The decorated function.
        """

        @functools.wraps(f)
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

    def _schema_required(self, schema: dict):
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
        def wrapper(f: Callable):
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

    def _register_endpoints(self):
        """
        Registers all the needed endpoints for the resources.
        """

        # First, list-wise and simple-wise resource methods.

        def _to_uint(value, minv=0):
            try:
                return max(minv, int(value))
            except:
                return minv

        def _parse_order_by(value):
            if not value:
                value = []
            elif isinstance(value, str):
                value = value.split(',')

            result = []
            for element in value:
                element = element.strip()
                if not element:
                    raise ValueError("Invalid order_by field: empty name")
                direction = ASCENDING
                if element[0] == '-':
                    element = element[1:]
                    direction = DESCENDING
                result.append((element, direction))
            return result

        @self.route('/<string:resource>', methods=["GET"])
        @self._bearer_required
        def resource_read(resource: str):
            """
            Intended for list-type resources and simple-type resources.
            List-type resources use a cursor and return a list.
            Simple-type resources return a single element, or nothing / 404.
            :param resource: The intended resource name.
            :return: Flask-compatible responses.
            """

            # First, get the resource definition.
            resource_definition = self._resources.get(resource)
            if not resource_definition:
                return NOT_FOUND
            verbs = resource_definition["verbs"]
            db_name = resource_definition["db"]
            collection_name = resource_definition["collection"]
            collection = CLIENT[db_name][collection_name]
            filter = resource_definition["filter"]
            if verbs != "*" and "GET" not in verbs:
                return NOT_FOUND

            # Its "type" will be "list" or "simple".
            if resource_definition["type"] == "list":
                # Process a "list" resource.
                offset = _to_uint(request.args.get("offset"))
                limit = _to_uint(request.args.get("limit"), 1)
                order_by = _parse_order_by(request.args.get("order_by", resource_definition.get("order_by")))

                query = collection.find(filter=filter, projection=resource_definition.get("list_projection"))
                if order_by:
                    query = query.sort(order_by)
                if offset:
                    query = query.skip(offset)
                if limit:
                    query = query.limit(limit)

                return jsonify(list(query)), 200
            else:
                # Process a "simple" resource.
                element = collection.find_one(filter=filter, projection=resource_definition.get("projection"))
                if element:
                    return jsonify(element), 200
                else:
                    return NOT_FOUND

        @self.route('/<string:resource>', methods=["POST"])
        @self._bearer_required
        def resource_create(resource: str):
            """
            Intended for list-type resources and simple-type resources.
            List-type resources gladly accept new content (a single
            new element from incoming body).
            Simple-type resources only accept new content (a single
            new element from incoming body as well) if no previous
            content exists. Otherwise, they return conflict / 409.
            :param resource: The intended resource name.
            :return: Flask-compatible responses.
            """

        @self.route('/<string:resource>/~<string:method>', methods=["GET"])
        @self._bearer_required
        def resource_view(resource: str, method: str):
            """
            Intended for list-type resources and simple-type resources.
            Implementations should operate over {collection}.find() for
            list resources, and over {collection}.find_one() for simple
            resources. The operation must be read-only.
            :param resource: The intended resource name.
            :param method: The method to invoke.
            :return: Flask-compatible responses.
            """

        @self.route('/<string:resource>/~<string:method>', methods=["POST"])
        @self._bearer_required
        def resource_operation(resource: str, method: str):
            """
            Intended for list-type resources and simple-type resources.
            Implementations should operate over {collection}.find() for
            list resources, and over {collection}.find_one() for simple
            resources. The operation can make changes.
            :param resource: The intended resource name.
            :param method: The method to invoke.
            :return: Flask-compatible responses.
            """

        @self.route('/<string:resource>', methods=["PUT"])
        @self._bearer_required
        def resource_read(resource: str):
            """
            Intended for simple-type resources. Replaces the element, if
            it exists (otherwise, returns a 404 error) with a new one, from
            the incoming json body.
            :param resource: The intended resource name.
            :return: Flask-compatible responses.
            """

        @self.route('/<string:resource>', methods=["PATCH"])
        @self._bearer_required
        def resource_create(resource: str):
            """
            Intended for simple-type resources. Updates the element, if
            it exists (otherwise, returns a 404 error) with new data from
            the incoming json body.
            :param resource: The intended resource name.
            :return: Flask-compatible responses.
            """

        @self.route('/<string:resource>', methods=["DELETE"])
        @self._bearer_required
        def resource_create(resource: str):
            """
            Intended for simple-type resources. Deletes the element, if
            it exists (otherwise, returns a 404 error).
            :param resource: The intended resource name.
            :return: Flask-compatible responses.
            """

        # Second, element-wise resource methods.

        @self.route('/<string:resource>/<regex("[a-f0-9]{24}"):object_id>', methods=["GET"])
        @self._bearer_required
        def item_resource_read(resource: str, object_id: str):
            """
            Reads an element from a list, or returns nothing / 404.
            :param resource: The intended resource name.
            :param object_id: The element id.
            :return: Flask-compatible responses.
            """

        @self.route('/<string:resource>/<regex("[a-f0-9]{24}"):object_id>', methods=["PUT"])
        @self._bearer_required
        def item_resource_replace(resource: str, object_id: str):
            """
            Replaces an element from a list, if it exists (or
            returns nothing / 404) with a new one from the
            incoming json body.
            :param resource: The intended resource name.
            :param object_id: The element id.
            :return: Flask-compatible responses.
            """

        @self.route('/<string:resource>/<regex("[a-f0-9]{24}"):object_id>', methods=["PATCH"])
        @self._bearer_required
        def item_resource_update(resource: str, object_id: str):
            """
            Updates an element from a list, if it exists (or
            returns nothing / 404) with new data from the
            incoming json body.
            :param resource: The intended resource name.
            :param object_id: The element id.
            :return: Flask-compatible responses.
            """

        @self.route('/<string:resource>/<regex("[a-f0-9]{24}"):object_id>', methods=["DELETE"])
        @self._bearer_required
        def item_resource_delete(resource: str, object_id: str):
            """
            Deletes an element from a list, if it exists (or
            returns nothing / 404).
            :param resource: The intended resource name.
            :param object_id: The element id.
            :return: Flask-compatible responses.
            """

        @self.route('/<string:resource>/<regex("[a-f0-9]{24}"):object_id>/~<string:method>', methods=["GET"])
        @self._bearer_required
        def item_resource_view(resource: str, object_id: str, method: str):
            """
            Implementation should operate over {collection}.find_one(
                {"_id": ObjectId(object_id)}
            ). This operation must be read-only.
            :param resource: The intended resource name.
            :param object_id: The element id.
            :param method: The method to invoke.
            :return: Flask-compatible responses.
            """

        @self.route('/<string:resource>/<regex("[a-f0-9]{24}"):object_id>/~<string:method>', methods=["POST"])
        @self._bearer_required
        def item_resource_operation(resource: str, object_id: str, method: str):
            """
            Implementation should operate over {collection}.find_one(
                {"_id": ObjectId(object_id)}
            ). This operation can make changes.
            :param resource: The intended resource name.
            :param object_id: The element id.
            :param method: The method to invoke.
            :return: Flask-compatible responses.
            """

# Results: {"code": "not-found"}, 404