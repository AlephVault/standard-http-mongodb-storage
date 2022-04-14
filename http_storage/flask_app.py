import typing as t
from datetime import date, datetime
from bson import ObjectId
from cerberus import Validator, TypeDefinition
from flask import Flask, current_app as app
from flask.json import JSONEncoder


_DATETIME_FORMATS = [
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
]
_DATE_FORMAT = "%Y-%m-%d"


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
