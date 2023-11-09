from utility.log import Log

log = Log(__name__)


# Fetches all the items in a dictionary whole value is also a dictionary
getdict = lambda x: {k: v for (k, v) in x.items() if isinstance(v, dict)}

# Returns whether an element in the list is a dictionary or not
isdict = lambda x: [isinstance(i, dict) for i in x]


def get_values(key, dictionary):
    """
    Find list of values for a particular key from a nested dictionary.

    Args:
        key: the key for which values need to be found
        dictionary: the dictionary in which the values needs to be searched

    Returns:
        The values for the given key from a nested dictionary
    """
    for k, v in dictionary.items():
        if k == key:
            yield v
        elif isinstance(v, dict):
            for result in get_values(key, v):
                yield result
        elif isinstance(v, list):
            for d in v:
                if isinstance(d, dict):
                    for result in get_values(key, d):
                        yield result
                else:
                    yield d


def get_first_value(key, dictionary):
    """
    Retrieve first value of a particular key from a nested dictionary.

    Args:
        key: the key for which values need to be found
        dictionary: the dictionary in which the values needs to be searched

    Returns:
        The first value for the given key from a nested dictionary
    """
    return str(list(get_values(key, dictionary))[0])


def deep_update(dictionary, *update_dictionary):
    """
    Update nested dictionary.

    Args:
        dictionary: input nested dictionary
        update_dictionary: nested dictionary to be updated

    Returns:
        Updated dictionary

    Note: Works only in the case of dictionary of dictionaries.
    Does not work when there is a dictionary of list of dictionaries.
    """
    updated_mapping = dictionary.copy()
    for updating_mapping in update_dictionary:
        for k, v in updating_mapping.items():
            if (
                k in updated_mapping
                and isinstance(updated_mapping[k], dict)
                and isinstance(v, dict)
            ):
                updated_mapping[k] = deep_update(updated_mapping[k], v)
            else:
                updated_mapping[k] = v
    return updated_mapping
