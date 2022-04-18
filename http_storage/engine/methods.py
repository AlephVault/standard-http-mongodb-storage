import os
from bson import ObjectId
from typing import NamedTuple, Optional, List, Union, Dict, Tuple
from pymongo import ASCENDING, DESCENDING
from pymongo.collection import Collection


MAX_RESULTS = max(1, int(os.getenv('MAX_RESULTS', '20')))
_NOT_FOUND = object()


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
    :param filter: An optional filter to use. The idea behind this filter is to be static, preset.
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


def list_item_get(collection: Collection, object_id: ObjectId, filter: Optional[dict] = None,
                  path: Optional[List[Tuple[str, Optional[Union[str, int]]]]] = None,
                  projection: Optional[Union[List, Dict[str, Union[int, bool]]]] = None):
    """
    Gets a particular object from the list.
    :param collection: The collection to get a document from.
    :param filter: An optional filter to use. The idea behind this filter is to be static, preset.
    :param object_id: The particular _id to lookup. It will be added to the optional filter, if any.
    :param path: The de-referencing path to use to get a part of the document.
    :param projection: The projection to use on the last part of the path.
    :return: A tuple telling (element: dict, found: bool), where the element is a document or
      a part of it (depending on how it was filtered using the path), and the `found` flag tells
      whether the element and all the internal path was found or not.
    """

    filter = filter or {}
    filter['_id'] = object_id
    # The projection will be a MongoDB-compatible projection, typically.
    # If using a weak path, the projection will consist of literal fields
    # only, and not weird criteria like $-prefixed operators and will be
    # used MANUALLY, later.
    if not path:
        element = collection.find_one(filter=filter, projection=projection)
        # Return (element, True) or (None, False).
        return element, element is not None
    else:
        element = collection.find_one(filter=filter)
        # If the element is None, return (None, False).
        if element is None:
            return None, False
        # The path has to be traversed. Each step is to be considered as a tuple
        # of (field, subscript or None). A special criteria will be used when
        # traversing everything.
        part = element
        for field, subscript in path:
            # If part is None, then we come from a previous iteration and, in
            # this case, this means that we must return (None, False), as
            # traversal cannot follow.
            if part is None:
                return None, False
            # Then, the field is retrieved. If the field is not present, then
            # we return (None, False), as if the object was not found at all.
            part = part.get(field, _NOT_FOUND)
            if part is _NOT_FOUND:
                return None, False
            # If there is a subscript, then we attempt subscript access. If
            # we get an error, we return (None, False), as if the object was
            # not found at all.
            if subscript is not None:
                try:
                    part = part[subscript]
                except:
                    return None, False
        # If the element is null, we return (None, True). Otherwise, if the
        # element to retrieve is a dict, we apply the projection (if present).
        if part is None:
            return None, True
        if not (isinstance(part, dict) and projection):
            return part, True
        # The projection goes like this:
        # 1. The projection may be a list of fields or a dict.
        # 2. The list version will be considered an inclusion: it will be
        #    considered (only, [...]).
        # 3. Each key in the dictionary will be {key: include} where the
        #    include flag will be treated as boolean. It is an error to
        #    have different boolean values in the flags.
        if isinstance(projection, list):
            projection = {k: True for k in projection}
        if not isinstance(projection, dict):
            raise TypeError("The projection must be a list or a dict")
        flags = set(bool(v) for v in projection.values())
        if len(flags) >= 2:
            raise ValueError("The dict-format projection must not have both exclusions and inclusions")
        # Get the only involved element in the flags set. It will be used
        # to distinguish whether an exclusion or an inclusion has to be done.
        flag = flags.pop()
        included = {}
        for k in projection.keys():
            included[k] = part.pop(k, None)
        # Finally, we return either the included or excluded part, according
        # to the flag value.
        return included if flag else part
