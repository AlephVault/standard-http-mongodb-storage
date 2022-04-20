import os
from cerberus import schema_registry


METHOD = {
    "type": {
        "type": "string",
        "allowed": ["view", "operation"]
    }
}
schema_registry.add("http_storage.schemas.method", METHOD)


RESOURCE = {
    "type": {
        "type": "string",
        "required": True,
        "allowed": ["list", "simple"]
    },
    "db": {
        "type": "string",
        "required": True,
        "regex": "[a-zA-Z][a-zA-Z0-9_-]+"
    },
    "collection": {
        "type": "string",
        "required": True,
        "regex": "[a-zA-Z][a-zA-Z0-9_-]+"
    },
    "methods": {
        "type": "dict",
        "default_setter": lambda doc: {},
        "valuesrules": {
            "type": "dict",
            "schema": "http_storage.schemas.method",
        },
        "keysrules": {
            "type": "string",
            "regex": "[a-zA-Z][a-zA-Z0-9_-]+"
        }
    },
    "list_verbs": {
        "type": "list",
        # Note: default_setter does not work since it is always processed
        #       and even when the dependency is not satisfied.
        "allowed": ['create', 'list', 'read', 'replace', 'update', 'delete'],
        "dependencies": {
            "type": "list"
        }
    },
    "single_verbs": {
        "type": "list",
        # Note: default_setter does not work since it is always processed
        #       and even when the dependency is not satisfied.
        "allowed": ['create', 'read', 'replace', 'update', 'delete'],
        "dependencies": {
            "type": "simple"
        }
    }
}
schema_registry.add("http_storage.schemas.resource", RESOURCE)


SETTINGS = {
    "connection": {
        "type": "dict",
        "default_setter": lambda doc: {},
        "schema": {
            "host": {
                "type": "string",
                "default_setter": lambda doc: os.getenv('MONGODB_HOST', 'localhost')
            },
            "port": {
                "type": "integer",
                "default_setter": lambda doc: int(os.getenv('MONGODB_PORT', '27017'))
            },
            "user": {
                "type": "string",
                "default_setter": lambda doc: os.getenv('MONGODB_USER', '')
            },
            "password": {
                "type": "string",
                "default_setter": lambda doc: os.getenv('MONGODB_PASSWORD', '')
            }
        }
    },
    "auth": {
        "type": "dict",
        "default_setter": lambda doc: {},
        "schema": {
            "db": {
                "type": "string",
                "regex": "[a-zA-Z][a-zA-Z0-9_-]+",
                "default_setter": lambda doc: os.getenv("APP_AUTH_DB", "http_storage")
            },
            "collection": {
                "type": "string",
                "regex": "[a-zA-Z][a-zA-Z0-9_-]+",
                "default_setter": lambda doc: os.getenv("APP_AUTH_DB", "auth")
            },
        }
    },
    "resources": {
        "type": "dict",
        "required": True,
        "valuesrules": {
            "type": "dict",
            "schema": "http_storage.schemas.resource",
        },
        "keysrules": {
            "type": "string",
            "regex": "[a-zA-Z][a-zA-Z0-9_-]+"
        }
    }
}
schema_registry.add("http_storage.schemas.settings", SETTINGS)
