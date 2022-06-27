def _get_module(
    obj,
    module,
    service,
):
    """
    Module to coordinate logic to classes and trigger methods based on user input
    provided in suite file using DFS.
    Args:
        obj(object)   :  object of a class
        module(str)   :  name of the module excluding .py
        service(str)  :  method name

    Returns:
        method() : the path of the file through which we can execute the sdk layer
    """
    if type(obj).__module__ == "builtins":
        return None
    if str(obj.__class__.__name__).lower().replace("_", "") == module.lower():
        method = getattr(obj, service, None)
        if method:
            return method
        return None

    else:
        for atrb, val in obj.__dict__.items():
            if not atrb.startswith("__"):
                obj_atrb = getattr(obj, atrb)
                method = _get_module(obj_atrb, module, service)
                if method:
                    return method
        return None


def get_module(**kw):
    obj = kw.get("obj")
    component = kw.get("component")
    module = kw.get("module")
    service = kw.get("service")
    obj = getattr(obj, component)
    return _get_module(obj, module, service)


# kw = {
#     "module":"cephadm",
#     "service":"bootstrap",
#     "component":"cephadm",
#     "obj": CLI()
# }

# m = get_module(kw=kw)
# print(m)
# m()
