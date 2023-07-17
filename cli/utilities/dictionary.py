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
