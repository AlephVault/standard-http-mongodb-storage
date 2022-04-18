import os
from typing import NamedTuple, Optional, List, Union, Dict
from pymongo import ASCENDING, DESCENDING
from pymongo.collection import Collection


MAX_RESULTS = max(1, int(os.getenv('MAX_RESULTS', '20')))


class Cursor(NamedTuple):
    """
    A cursor has 3 fields to use: an offset and a limit (defaults to 0) and
    a limit (default to whatever is set in MAX_RESULTS or a default of 20
    if that setting is absent), and perhaps a list of fields to order by
    (defaults to None).
    """

    order_by: Optional[List[str]]
    offset: Optional[int]
    limit: Optional[int]

    def sort_criteria(self):
        """
        Converts a list of strings to a list of sort criteria in pymongo
        format (list of (field_name, ASCENDING|DESCENDING)).
        :return: The converted criteria.
        """

        if not self.order_by:
            return []

        result = []
        for element in self.order_by:
            element = element.strip()
            if not element:
                raise ValueError("Invalid order_by field: empty name")
            direction = ASCENDING
            if element[0] == '-':
                element = element[1:]
                direction = DESCENDING
            result.append((element, direction))
        return result


def list_get(collection: Collection, filter: Optional[dict] = None, cursor: Optional[Cursor] = None,
             projection: Optional[Union[List, Dict[str, Union[int, bool]]]] = None):
    """
    Gets, from a collection, a particular chunk (or all the elements) and
    using perhaps a particular projection (or not using any projection at
    all and retrieving everything instead).
    :param collection: The PyMongo collection object to retrieve the data from.
    :param filter: A filter to use. The idea behind this filter is to be static, preset.
    :param cursor: The optional cursor to use. This cursor is a
    :param projection: The optional projection to use.
    :return: A list of elements.
    """

    skip = 0
    limit = MAX_RESULTS
    sort = None

    if cursor is not None:
        if cursor.offset is not None:
            skip = max(0, cursor.offset)
        if cursor.limit is not None:
            skip = max(1, cursor.limit)
        if cursor.order_by is not None:
            sort = cursor.sort_criteria()
    return list(collection.find(filter=filter, projection=projection, sort=sort, skip=skip, limit=limit))


def list_post(collection: Collection, document: dict):
    """
    Adds a new document to the collection. Returns the insert id, or an internal error.
    :param collection: The collection to insert a document into.
    :param document: The document to insert.
    :return: The inserted id.
    """

    result = collection.insert_one(document, bypass_document_validation=True)
    return result.inserted_id
