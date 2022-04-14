import datetime
from bson import ObjectId
from flask import current_app, request, jsonify, make_response
from ..mongo_client import client


def bearer_required(f):
    """
    Requires a valid "Authorization: Bearer xxxxx..." header.
    :param f: The function to invoke.
    :return: The decorated function.
    """

    def wrapper(*args, **kwargs):
        # Get the auth settings.
        auth_db = getattr(current_app, 'auth_db', None)
        auth_table = getattr(current_app, 'auth_table', None)
        if not (auth_db and auth_table):
            return make_response(jsonify({'code': 'service:misconfigured'}), 500)
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
                                                      'valid_until': {"$not": {"$lt": datetime.datetime.now()}}})
        if not token:
            return make_response(jsonify({'code': 'authorization:not-found'}), 401)
        # If the validation passed, then we invoke the decorated function.
        return f(*args, **kwargs)
    return wrapper
