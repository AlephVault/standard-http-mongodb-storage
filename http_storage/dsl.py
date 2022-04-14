from typing import NamedTuple, List, Union


class Store(NamedTuple):
    """
    A store defines in which database name and which collection name
    the data is stored. This data has a defined schema (which can
    accept a variety of formats, or just a single one).
    """

    db_name: str
    collection_name: str
    schema: dict


class ListResource(NamedTuple):
    """
    A definition of a list resource involves the following elements:
    - The resource slug (unique in its resource level).
    - The related store, the projections (optional: a falsy list on either will involve
      all the elements - one projection exists for single-element retrieval and another
      one for list-elements retrieval - "_id" is always included).
    - The allowed methods (for list: GET, POST; for item: GET, PUT, PATCH, DELETE).
    - The item-related sub-resources.
    - The list-related sub-resources.
    """

    slug: str
    store: Store
    list_projection: List[str]
    item_projection: List[str]
    allow_create: bool
    allow_list: bool
    allow_item_read: bool
    allow_item_update: bool
    allow_item_replace: bool
    allow_item_delete: bool
    item_resources: List[Union['ListResource', 'SimpleResource', 'WeakResource']]
    list_resources: List[Union['ListResource', 'SimpleResource']]


class SimpleResource(NamedTuple):
    """
    A definition of a simple resource involves the following elements:
    - The resource slug (unique in its resource level).
    - The related store, the projection (optional: a falsy list on it will involve all
      the elements - "_id" is always included).
    - The allowed methods (POST, GET, PUT, PATCH, DELETE).
    - The sub-resources.
    """

    slug: str
    store: Store
    projection: List[str]
    allow_create: bool
    allow_read: bool
    allow_update: bool
    allow_replace: bool
    allow_delete: bool
    resources: List[Union['ListResource', 'SimpleResource', 'WeakResource']]


class WeakResource(NamedTuple):
    """
    A definition of a weak resource involves the following elements:
    - The resource slug (unique in its resource level).
    - The member in a parent object. This resource does not have its own store but depends
      on a single element (either by itself, or an element of a list, or another weak
      element) and extracts one particular member of that parent object. From that result,
      it can specify a projection (where empty means "all the fields inside that result",
      and no "_id" field will be selected).
    - The allowed methods (GET, PUT, PATCH).
    - The sub-resources.
    """

    slug: str
    parent_member: str
    projection: List[str]
    allow_read: bool
    allow_patch: bool
    allow_replace: bool
    resources: List[Union['WeakResource']]


# TODO Incomplete: Define callback formats for the methods, so the user can run their own
# TODO logic if the first one has to be overridden totally or partially, or conditioned
# TODO in some way (also: complemented in some way).
