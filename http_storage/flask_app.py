import re
import json
import logging
import functools
from datetime import datetime
from typing import Callable
from urllib.parse import quote_plus
from bson import ObjectId
from flask import Flask, make_response, request
from flask.json import jsonify
from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.collection import Collection
from .core.json import MongoDBEnhancedEncoder
from .core.validation import MongoDBEnhancedValidator
from .engine.schemas import *


LOGGER = logging.getLogger(__name__)
_PROJECTION_RX = re.compile(r"^-?([a-zA-Z][a-zA-Z0-9_-]+)(,[a-zA-Z][a-zA-Z0-9_-]+)*$")


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

    def __init__(self, settings: dict, validator_class: type = None, *args, **kwargs):
        """
        Checks a validator_class is properly configured, as well as the auth_db / auth_table.

        :param settings: The schema being used for this app.
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
        if not validator.validate(settings):
            raise ImproperlyConfiguredError(f"Validation errors on resources DSL: {validator.errors}")
        self._settings = validator.document
        self._client = self._build_client(self._settings["connection"])
        self._resource_validators = {}
        for key, resource in self._settings["resources"].items():
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

    def _build_client(self, connection):
        """
        Builds a client from the connection settings.
        :param connection: The connection settings.
        :return: The mongo client.
        """

        host = connection["host"].strip()
        port = connection["port"]
        user = connection["user"].strip()
        password = connection["password"]

        if not user or not password:
            raise ImproperlyConfiguredError("Missing MongoDB user or password")
        return MongoClient("mongodb://%s:%s@%s:%s" % (quote_plus(user), quote_plus(password), host, port))

    def _bearer_required(self, f: Callable):
        """
        Requires a valid "Authorization: Bearer xxxxx..." header.
        :param f: The function to invoke.
        :return: The decorated function.
        """

        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            # Get the auth settings.
            auth_db = self._settings["auth"]["db"]
            auth_table = self._settings["auth"]["collection"]
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
            token = self._client[auth_db][auth_table].find_one({
                "_id": ObjectId(token), "valid_until": {"$not": {"$lt": datetime.now()}}
            })
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
            resource_definition = self._settings["resources"].get(resource)
            if not resource_definition:
                return make_response(jsonify({"code": "not-found"}), 404)
            verbs = resource_definition["verbs"]
            db_name = resource_definition["db"]
            collection_name = resource_definition["collection"]
            collection = self._client[db_name][collection_name]
            filter = resource_definition["filter"]
            if resource_definition["soft_delete"]:
                filter = {**filter, "_deleted": False}
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

        def _parse_projection(projection):
            if projection is None:
                return None
            elif isinstance(projection, (list, tuple, dict)):
                return projection
            elif isinstance(projection, str):
                try:
                    # 1. attempt json, and pass directly.
                    return json.loads(projection)
                except ValueError:
                    # 2. attempt a csv format.
                    if projection == "":
                        raise TypeError("Invalid projection value")
                    elif projection == "*":
                        # Use full object.
                        return None
                    elif not _PROJECTION_RX.match(projection):
                        raise TypeError("Invalid projection value")

                    # Parse as a dictionary.
                    include = True
                    if projection[0] == "-":
                        include = False
                        projection = projection[1:]
                    return {p: include for p in projection}
            else:
                raise TypeError("Invalid projection value")

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

            # Get the projection to use.
            projection = _parse_projection(request.args.get('projection') or resource_definition.get("list_projection"))

            # Its "type" will be "list" or "simple".
            if resource_definition["type"] == "list":
                # Process a "list" resource.
                offset = _to_uint(request.args.get("offset"))
                limit = _to_uint(request.args.get("limit"), 1)
                order_by = _parse_order_by(request.args.get("order_by", resource_definition.get("order_by")))

                query = collection.find(filter=filter, projection=projection)
                if order_by:
                    query = query.sort(order_by)
                if offset:
                    query = query.skip(offset)
                if limit:
                    query = query.limit(limit)

                return make_response(jsonify(list(query)), 200)
            else:
                # Process a "simple" resource.
                element = collection.find_one(filter=filter, projection=projection)
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
                # Its "type" will be "list" or "simple".
                if resource_definition["type"] != "list" and collection.find_one(filter):
                    return make_response(jsonify({"code": "already-exists"}), 409)
                else:
                    result = collection.insert_one(validator.document)
                    return make_response(jsonify({"id": result.inserted_id}), 201)
            else:
                return make_response(jsonify({"code": "schema:invalid", "errors": validator.errors}), 400)

        @self.route("/<string:resource>/~<string:method>", methods=["GET", "POST"])
        @self._capture_unexpected_errors
        @self._using_resource
        @self._bearer_required
        def resource_method(resource: str, resource_definition: dict, db_name: str, collection_name: str,
                            collection: Collection, filter: dict, method: str):
            """
            Intended for list-type resources and simple-type resources.
            Implementations should operate over {collection}.find() for
            list resources, and over {collection}.find_one() for simple
            resources. The operation must be read-only for GET verb.
            :return: Flask-compatible responses.
            """

            try:
                method_entry = resource_definition["methods"][method]
                if request.method == "GET":
                    if method_entry["type"] != "view":
                        return make_response(jsonify({"code": "not-found"}), 404)
                else:
                    if method_entry["type"] != "operation":
                        return make_response(jsonify({"code": "not-found"}), 404)
            except KeyError:
                return make_response(jsonify({"code": "not-found"}), 404)

            # Getting the appropriate instance.
            instance = method_entry["handler"]()

            # Invoke the method
            return instance(self._client, resource, method, db_name, collection_name, filter)

        @self.route("/<string:resource>", methods=["PUT"])
        @self._capture_unexpected_errors
        @self._using_resource
        @self._bearer_required
        def resource_replace(resource: str, resource_definition: dict, db_name: str, collection_name: str,
                             collection: Collection, filter: dict):
            """
            Intended for simple-type resources. Replaces the element, if
            it exists (otherwise, returns a 404 error) with a new one, from
            the incoming json body.
            :return: Flask-compatible responses.
            """

            # Process a "simple" resource.
            element = collection.find_one(filter=filter)
            if element:
                validator = self._resource_validators[resource]
                if validator.validate(request.json):
                    collection.replace_one(filter, validator.document, upsert=False)
                    return make_response(jsonify({"code": "ok"}), 200)
                else:
                    return make_response(jsonify({"code": "schema:invalid", "errors": validator.errors}), 400)
            else:
                return make_response(jsonify({"code": "not-found"}), 404)

        @self.route("/<string:resource>", methods=["PATCH"])
        @self._capture_unexpected_errors
        @self._using_resource
        @self._bearer_required
        def resource_update(resource: str, resource_definition: dict, db_name: str, collection_name: str,
                            collection: Collection, filter: dict):
            """
            Intended for simple-type resources. Updates the element, if
            it exists (otherwise, returns a 404 error) with new data from
            the incoming json body.
            :return: Flask-compatible responses.
            """

            element = collection.find_one(filter=filter)
            if element:
                collection.update_one(filter, request.json, upsert=False)
                return make_response(jsonify({"code": "ok"}), 200)
            else:
                return make_response(jsonify({"code": "not-found"}), 404)

        @self.route("/<string:resource>", methods=["DELETE"])
        @self._capture_unexpected_errors
        @self._using_resource
        @self._bearer_required
        def resource_delete(resource: str, resource_definition: dict, db_name: str, collection_name: str,
                            collection: Collection, filter: dict):
            """
            Intended for simple-type resources. Deletes the element, if
            it exists (otherwise, returns a 404 error).
            :return: Flask-compatible responses.
            """

            if resource_definition["soft_delete"]:
                result = collection.update_one(filter, {"$set": {"_deleted": True}}, upsert=False)
                if result.modified_count:
                    return make_response(jsonify({"code": "ok"}), 200)
                else:
                    return make_response(jsonify({"code": "not-found"}), 404)
            else:
                result = collection.delete_one(filter)
                if result.deleted_count:
                    return make_response(jsonify({"code": "ok"}), 200)
                else:
                    return make_response(jsonify({"code": "not-found"}), 404)

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

            # Process a "simple" resource.
            projection = _parse_projection(request.args.get('projection') or resource_definition.get("projection"))
            element = collection.find_one(filter={**filter, "_id": ObjectId(object_id)}, projection=projection)
            if element:
                return make_response(jsonify(element), 200)
            else:
                return make_response(jsonify({"code": "not-found"}), 404)

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

            element = collection.find_one(filter={**filter, "_id": ObjectId(object_id)})
            if element:
                validator = self._resource_validators[resource]
                if validator.validate(request.json):
                    collection.replace_one(filter, validator.document, upsert=False)
                    return make_response(jsonify({"code": "ok"}), 200)
                else:
                    return make_response(jsonify({"code": "schema:invalid", "errors": validator.errors}), 400)
            else:
                return make_response(jsonify({"code": "not-found"}), 404)

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

            element = collection.find_one(filter={**filter, "_id": ObjectId(object_id)})
            if element:
                collection.update_one(filter, request.json, upsert=False)
                return make_response(jsonify({"code": "ok"}), 200)
            else:
                return make_response(jsonify({"code": "not-found"}), 404)


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

            filter = {**filter, "_id": ObjectId(object_id)}
            if resource_definition["soft_delete"]:
                result = collection.update_one(filter, {"$set": {"_deleted": True}}, upsert=False)
                if result.modified_count:
                    return make_response(jsonify({"code": "ok"}), 200)
                else:
                    return make_response(jsonify({"code": "not-found"}), 404)
            else:
                result = collection.delete_one(filter)
                if result.deleted_count:
                    return make_response(jsonify({"code": "ok"}), 200)
                else:
                    return make_response(jsonify({"code": "not-found"}), 404)

        @self.route("/<string:resource>/<regex('[a-f0-9]{24}'):object_id>/~<string:method>", methods=["GET", "POST"])
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

            try:
                method_entry = resource_definition["item_methods"][method]
                if request.method == "GET":
                    if method_entry["type"] != "view":
                        return make_response(jsonify({"code": "not-found"}), 404)
                else:
                    if method_entry["type"] != "operation":
                        return make_response(jsonify({"code": "not-found"}), 404)
            except KeyError:
                return make_response(jsonify({"code": "not-found"}), 404)

            # Getting the appropriate instance.
            instance = method_entry["handler"]()

            # Invoke the method
            return instance(self._client, resource, method, db_name, collection_name, filter, ObjectId(object_id))
