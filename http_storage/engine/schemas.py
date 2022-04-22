import os
from cerberus import schema_registry


METHOD = {
    "type": {
        "type": "string",
        "required": True,
        "allowed": ["view", "operation"]
    },
    "handler": {
        "type": "method",
        "required": True
    }
}
schema_registry.add("http_storage.schemas.method", METHOD)


ITEM_METHOD = {
    "type": {
        "type": "string",
        "required": True,
        "allowed": ["view", "operation"]
    },
    "handler": {
        "type": "item-method",
        "required": True
    }
}
schema_registry.add("http_storage.schemas.item-method", ITEM_METHOD)


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
        # Default is None.
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
    "filter": {
        "type": "dict",
        "default_setter": lambda doc: {},
    },
    "projection": {
        # Intended for element and simple.
        "anyof": [
            {"type": "list"},
            {"type": "dict"}
        ]
    },
    "order_by": {
        "dependencies": {"type": "list"},
        "type": "list",
        "schema": {
            "type": "string",
            "regex": "-?[a-zA-Z][a-zA-Z0-9_-]+"
        }
    },
    "list_projection": {
        # Intended for elements in list pages.
        "dependencies": {"type": "list"},
        "anyof": [
            {"type": "list"},
            {"type": "dict"}
        ]
    },
    "methods": {
        "type": "dict",
        "default_setter": lambda doc: {},
        "keysrules": {
            "type": "string",
            "regex": "[a-zA-Z][a-zA-Z0-9_-]+"
        },
        "valuesrules": {
            "type": "dict",
            "schema": "http_storage.schemas.method",
        },
    },
    "item_methods": {
        "type": "dict",
        # No default setter will be given here, since it collides
        # with the dependency setting (breaks for "type": "simple").
        "dependencies": {"type": "list"},
        "keysrules": {
            "type": "string",
            "regex": "[a-zA-Z][a-zA-Z0-9_-]+"
        },
        "valuesrules": {
            "type": "dict",
            "schema": "http_storage.schemas.item-method",
        },
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
    },
    "schema": {
        "type": "dict",
        "default_setter": lambda doc: {}
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
                "empty": False,
                "default_setter": lambda doc: os.getenv('MONGODB_HOST', 'localhost')
            },
            "port": {
                "type": "integer",
                "empty": False,
                "default_setter": lambda doc: int(os.getenv('MONGODB_PORT', '27017'))
            },
            "user": {
                "type": "string",
                "empty": False,
                "default_setter": lambda doc: os.getenv('MONGODB_USER', '')
            },
            "password": {
                "type": "string",
                "empty": False,
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
