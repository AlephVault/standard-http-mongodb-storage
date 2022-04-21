import os
from cerberus import schema_registry


SIMPLE_METHOD = {
    "type": {
        "type": "string",
        "required": True,
        "allowed": ["view", "operation"]
    }
}
schema_registry.add("http_storage.schemas.simple-method", SIMPLE_METHOD)


LIST_METHOD = {
    "type": {
        "type": "string",
        "required": True,
        "allowed": ["view", "operation"]
    },
    "scope": {
        "type": "string",
        "required": True,
        "allowed": ["list", "item"]
    }
}
schema_registry.add("http_storage.schemas.list-method", LIST_METHOD)


PARTIAL = {
    "field_type": {
        "type": "string",
        "required": True,
        "allowed": ["scalar", "list", "dict"]
    },
    "field_name": {
        "type": "string",
        "required": True,
        "regex": "[a-zA-Z][a-zA-Z0-9_-]+"
    },
    "children": {
        "type": "dict",
        "valuesrules": {
            "type": "dict",
            "schema": "http_storage.schemas.partial",
        },
        "keysrules": {
            "type": "string",
            "regex": "[a-zA-Z][a-zA-Z0-9_-]+"
        }
    }
}
schema_registry.add("http_storage.schemas.partial", PARTIAL)


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
        "keysrules": {
            "type": "string",
            "regex": "[a-zA-Z][a-zA-Z0-9_-]+"
        },
        "anyof": [
            {
                "dependencies": {"type": "list"},
                "valuesrules": {
                    "type": "dict",
                    "schema": "http_storage.schemas.list-method",
                },
            },
            {
                "dependencies": {"type": "simple"},
                "valuesrules": {
                    "type": "dict",
                    "schema": "http_storage.schemas.simple-method",
                },
            }
        ]
    },
    "verbs": {
        "empty": False,
        "default_setter": lambda doc: '*',
        "anyof": [
            {
                "type": "string",
                "allowed": ["*"]
            },
            {
                "type": "list",
                "dependencies": {"type": "list"},
                "allowed": ['create', 'list', 'read', 'replace', 'update', 'delete']
            },
            {
                "type": "list",
                "dependencies": {"type": "simple"},
                "allowed": ['create', 'read', 'replace', 'update', 'delete']
            }
        ]
    },
    "partials": {
        "type": "dict",
        "default_setter": lambda doc: {},
        "valuesrules": {
            "type": "dict",
            "schema": "http_storage.schemas.partial",
        },
        "keysrules": {
            "type": "string",
            "regex": "[a-zA-Z][a-zA-Z0-9_-]+"
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
