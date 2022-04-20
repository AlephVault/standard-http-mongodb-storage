from typing import Optional, List


def parse_path(paths_dsn: Optional[dict] = None, extra_path: Optional[List[str]] = None):
    """
    Parses a path, which can be understood as an URL chunk.
    :param extra_path: The path to parse. By this point, the path comes
      as a list of strings (already url-decoded).
    :param paths_dsn: The paths DSN being used. By this point, the dsn
      format is completely valid.
    :return: If the parse was appropriate (in format and in contrast
      to the DSN in use), returns the path and a flag telling whether
      the parse was successful. Otherwise, returns (None, False).
    """

    if paths_dsn:
        # A path should be present.
        if not extra_path:
            return None, False

        result = []
        expecting_dict_key = False
        expecting_optional_list_index = False
        try:
            it = iter(extra_path)
            while True:
                # First, a prior check: if it was expecting an index,
                # which is only optional of the last element was a list,
                # then resolve the index now.
                if expecting_optional_list_index:
                    # Add the next chunk as an integer index.
                    result.append(int(next(it)))
                    # Clear the flag to not expect a subscript anymore.
                    expecting_optional_list_index = False

                # Then, if the current resource cannot be found among
                # the list of the (current level's) paths_dsn, fail.
                # This covers even when such list/set/mapping is empty.
                chunk = next(it)
                path_dsn = paths_dsn.get(chunk, {})
                if not path_dsn:
                    return None, False
                # Extract the field to use from database, and also the
                # expected type of the field.
                field = path_dsn['field']
                ftype = path_dsn['type']
                result.append(field)
                if ftype == 'scalar':
                    # The field will be treated as scalar (despite its
                    # true type). Nothing else to do here.
                    continue
                elif ftype == 'list':
                    # The field will be treated as list (and it will be
                    # expected to be a list in the document, and expect
                    # an integer index as the next chunk). Mark the flag
                    # to expect a subscript.
                    expecting_optional_list_index = True
                elif ftype == 'dict':
                    # The field will be treated as dict (and it will be
                    # expected to be a dict in the document, and expect
                    # a string index as the next chunk). Mark the flag
                    # to expect a subscript.
                    expecting_dict_key = True
                    # Add the next chunk as an integer index.
                    result.append(next(it))
                    # Clear the flag to not expect a subscript anymore.
                    expecting_dict_key = False
                else:
                    # This is just a marker - it will NEVER be reached.
                    return None, False
        except StopIteration:
            # The process stopped appropriately, unless it expected
            # a key or index. In that case, then fail.
            if expecting_dict_key:
                return None, False
            # Return appropriately, otherwise.
            return result, True
        except Exception:
            # No exception should be tolerated.
            return None, False
    else:
        # No path should be present.
        if extra_path:
            return None, False
        return None, True
