import logging
import functools
from datetime import datetime
from typing import Callable
from bson import ObjectId
from flask import Flask, make_response, request
from flask.json import jsonify
from pymongo import ASCENDING, DESCENDING
from pymongo.collection import Collection
from .core.json import MongoDBEnhancedEncoder
from .core.validation import MongoDBEnhancedValidator
from .engine.client import CLIENT
from .engine.schemas import *


LOGGER = logging.getLogger(__name__)


class ImproperlyConfiguredError(Exception):
    """
    Raised when the storage app is misconfigured.
    """


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
        # all the resources" metadata from it).
        if not (isinstance(self._validator_class, type) and issubclass(self._validator_class, MongoDBEnhancedValidator)):
            raise ImproperlyConfiguredError("Wrong or missing validator class")
        validator = self._validator_class("http_storage.schemas.settings")
        if not validator.validate(resources):
            raise ImproperlyConfiguredError(f"Validation errors on resources DSL: {validator.errors}")
        self._resources = validator.document
        self._resource_validators = {}
        for key, resource in self._resources.items():
            schema = resource["schema"]
            if not schema:
                raise ImproperlyConfiguredError(f"Validation errors on resource schema for key '{key}': it is empty")
            else:
                try:
                    self._resource_validators[key] = self._validator_class(schema)
                except:
                    raise ImproperlyConfiguredError(f"Validation errors on resource schema for key '{key}'")

        # Then, the base initialization must occur.
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
            auth_db = self._resources["auth"]["db"]
            auth_table = self._resources["auth"]["collection"]
            # Get the header. It must be "bearer {token}".
            authorization = request.headers.get("Authorization")
            if not authorization:
                return make_response(jsonify({"code": "authorization:missing-header"}), 401)
            # Split it, and expect it to be "bearer".
            try:
                scheme, token = authorization.split(" ")
                if scheme.lower() != "bearer":
                    return make_response(jsonify({"code": "authorization:bad-scheme"}), 400)
            except ValueError:
                return make_response(jsonify({"code": "authorization:syntax-error"}), 400)
            # Check the token.
            token = CLIENT[auth_db][auth_table].find_one({"_id": ObjectId(token),
                                                          "valid_until": {"$not": {"$lt": datetime.now()}}})
            if not token:
                return make_response(jsonify({"code": "authorization:not-found"}), 401)
            # If the validation passed, then we invoke the decorated function.
            return f(*args, **kwargs)
        return wrapper

    def _capture_unexpected_errors(self, f: Callable):
        """
        Logs and wraps the unexpected errors.
        :param f: The handler function to invoke.
        :return: A new handler which captures and logs any error
          and returns a 500.
        """

        def wrapper(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except:
                LOGGER.exception("An exception was occurred (don't worry! it was wrapped into a 500 error)")
                return jsonify({"code": "internal-error"}), 500
        return wrapper

    def _using_resource(self, f: Callable):
        """
        Wraps a handler to provide more data from the resource definition.
        Returns a 404 if the resource is not defined.
        :param f: The handler function to invoke.
        :return: A new handler which gets the resource and passes it
          to the wrapped handler.
        """

        @functools.wraps(f)
        def new_handler(resource: str, *args, **kwargs):
            resource_definition = self._resources.get(resource)
            if not resource_definition:
                return make_response(jsonify({"code": "not-found"}), 404)
            verbs = resource_definition["verbs"]
            db_name = resource_definition["db"]
            collection_name = resource_definition["collection"]
            collection = CLIENT[db_name][collection_name]
            filter = resource_definition["filter"]
            if verbs != "*" and request.method not in verbs:
                return make_response(jsonify({"code": "not-found"}), 404)
            return f(resource, resource_definition, db_name, collection_name, collection, filter, *args, **kwargs)
        return new_handler

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
                value = value.split(",")

            result = []
            for element in value:
                element = element.strip()
                if not element:
                    raise ValueError("Invalid order_by field: empty name")
                direction = ASCENDING
                if element[0] == "-":
                    element = element[1:]
                    direction = DESCENDING
                result.append((element, direction))
            return result

        @self.route("/<string:resource>", methods=["GET"])
        @self._capture_unexpected_errors
        @self._using_resource
        @self._bearer_required
        def resource_read(resource: str, resource_definition: dict, db_name: str, collection_name: str,
                          collection: Collection, filter: dict):
            """
            Intended for list-type resources and simple-type resources.
            List-type resources use a cursor and return a list.
            Simple-type resources return a single element, or nothing / 404.
            """

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

                return make_response(jsonify(list(query)), 200)
            else:
                # Process a "simple" resource.
                element = collection.find_one(filter=filter, projection=resource_definition.get("projection"))
                if element:
                    return make_response(jsonify(element), 200)
                else:
                    return make_response(jsonify({"code": "not-found"}), 404)

        @self.route("/<string:resource>", methods=["POST"])
        @self._capture_unexpected_errors
        @self._using_resource
        @self._bearer_required
        def resource_create(resource: str, resource_definition: dict, db_name: str, collection_name: str,
                            collection: Collection, filter: dict):
            """
            Intended for list-type resources and simple-type resources.
            List-type resources gladly accept new content (a single
            new element from incoming body).
            Simple-type resources only accept new content (a single
            new element from incoming body as well) if no previous
            content exists. Otherwise, they return conflict / 409.
            :return: Flask-compatible responses.
            """

            # Require the body to be json, and validate it.
            if not request.is_json:
                return make_response(jsonify({"code": "format:unexpected"}), 400)
            validator = self._resource_validators[resource]
            if validator.validate(request.json):
                result = collection.insert_one(validator.document)
                return make_response(jsonify({"id": result.inserted_id}), 201)
            else:
                return make_response(jsonify({"code": "schema:invalid", "errors": validator.errors}), 400)

        @self.route("/<string:resource>/~<string:method>", methods=["GET"])
        @self._capture_unexpected_errors
        @self._using_resource
        @self._bearer_required
        def resource_view(resource: str, resource_definition: dict, db_name: str, collection_name: str,
                          collection: Collection, filter: dict, method: str):
            """
            Intended for list-type resources and simple-type resources.
            Implementations should operate over {collection}.find() for
            list resources, and over {collection}.find_one() for simple
            resources. The operation must be read-only.
            :return: Flask-compatible responses.
            """

        @self.route("/<string:resource>/~<string:method>", methods=["POST"])
        @self._capture_unexpected_errors
        @self._using_resource
        @self._bearer_required
        def resource_operation(resource: str, resource_definition: dict, db_name: str, collection_name: str,
                               collection: Collection, filter: dict, method: str):
            """
            Intended for list-type resources and simple-type resources.
            Implementations should operate over {collection}.find() for
            list resources, and over {collection}.find_one() for simple
            resources. The operation can make changes.
            :return: Flask-compatible responses.
            """

        @self.route("/<string:resource>", methods=["PUT"])
        @self._capture_unexpected_errors
        @self._using_resource
        @self._bearer_required
        def resource_read(resource: str, resource_definition: dict, db_name: str, collection_name: str,
                          collection: Collection, filter: dict):
            """
            Intended for simple-type resources. Replaces the element, if
            it exists (otherwise, returns a 404 error) with a new one, from
            the incoming json body.
            :param resource: The intended resource name.
            :return: Flask-compatible responses.
            """

        @self.route("/<string:resource>", methods=["PATCH"])
        @self._capture_unexpected_errors
        @self._using_resource
        @self._bearer_required
        def resource_create(resource: str, resource_definition: dict, db_name: str, collection_name: str,
                            collection: Collection, filter: dict):
            """
            Intended for simple-type resources. Updates the element, if
            it exists (otherwise, returns a 404 error) with new data from
            the incoming json body.
            :param resource: The intended resource name.
            :return: Flask-compatible responses.
            """

        @self.route("/<string:resource>", methods=["DELETE"])
        @self._capture_unexpected_errors
        @self._using_resource
        @self._bearer_required
        def resource_create(resource: str, resource_definition: dict, db_name: str, collection_name: str,
                            collection: Collection, filter: dict):
            """
            Intended for simple-type resources. Deletes the element, if
            it exists (otherwise, returns a 404 error).
            :return: Flask-compatible responses.
            """

        # Second, element-wise resource methods.

        @self.route("/<string:resource>/<regex('[a-f0-9]{24}'):object_id>", methods=["GET"])
        @self._capture_unexpected_errors
        @self._using_resource
        @self._bearer_required
        def item_resource_read(resource: str, resource_definition: dict, db_name: str, collection_name: str,
                               collection: Collection, filter: dict, object_id: str):
            """
            Reads an element from a list, or returns nothing / 404.
            :return: Flask-compatible responses.
            """

        @self.route("/<string:resource>/<regex('[a-f0-9]{24}'):object_id>", methods=["PUT"])
        @self._capture_unexpected_errors
        @self._using_resource
        @self._bearer_required
        def item_resource_replace(resource: str, resource_definition: dict, db_name: str, collection_name: str,
                                  collection: Collection, filter: dict, object_id: str):
            """
            Replaces an element from a list, if it exists (or
            returns nothing / 404) with a new one from the
            incoming json body.
            :return: Flask-compatible responses.
            """

        @self.route("/<string:resource>/<regex('[a-f0-9]{24}'):object_id>", methods=["PATCH"])
        @self._capture_unexpected_errors
        @self._using_resource
        @self._bearer_required
        def item_resource_update(resource: str, resource_definition: dict, db_name: str, collection_name: str,
                                 collection: Collection, filter: dict, object_id: str):
            """
            Updates an element from a list, if it exists (or
            returns nothing / 404) with new data from the
            incoming json body.
            :return: Flask-compatible responses.
            """

        @self.route("/<string:resource>/<regex('[a-f0-9]{24}'):object_id>", methods=["DELETE"])
        @self._capture_unexpected_errors
        @self._using_resource
        @self._bearer_required
        def item_resource_delete(resource: str, resource_definition: dict, db_name: str, collection_name: str,
                                 collection: Collection, filter: dict, object_id: str):
            """
            Deletes an element from a list, if it exists (or
            returns nothing / 404).
            :return: Flask-compatible responses.
            """

        @self.route("/<string:resource>/<regex('[a-f0-9]{24}'):object_id>/~<string:method>", methods=["GET"])
        @self._capture_unexpected_errors
        @self._using_resource
        @self._bearer_required
        def item_resource_view(resource: str, resource_definition: dict, db_name: str, collection_name: str,
                               collection: Collection, filter: dict, object_id: str, method: str):
            """
            Implementation should operate over {collection}.find_one(
                {"_id": ObjectId(object_id)}
            ). This operation must be read-only.
            :return: Flask-compatible responses.
            """

        @self.route("/<string:resource>/<regex('[a-f0-9]{24}'):object_id>/~<string:method>", methods=["POST"])
        @self._capture_unexpected_errors
        @self._using_resource
        @self._bearer_required
        def item_resource_operation(resource: str, resource_definition: dict, db_name: str, collection_name: str,
                                    collection: Collection, filter: dict, object_id: str, method: str):
            """
            Implementation should operate over {collection}.find_one(
                {"_id": ObjectId(object_id)}
            ). This operation can make changes.
            :return: Flask-compatible responses.
            """
