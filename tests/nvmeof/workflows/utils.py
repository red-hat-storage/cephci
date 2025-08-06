import time


def get_current_timestamp():
    return time.perf_counter(), time.asctime()


@staticmethod
def string_to_dict(string):
    """Parse ANA states from the string."""
    states = string.replace(" ", "").split(",")
    dict = {}
    for state in states:
        if not state:
            continue
        _id, _state = state.split(":")
        dict[int(_id)] = _state
    return dict
