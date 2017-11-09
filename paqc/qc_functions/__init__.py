# We'll want to import all QC functions in one fell swoop, so we'll add them
# dynamically to the __all__ list.

import importlib
import pkgutil
import re


def import_submodules(package):
    """ This function will help us load all QC functions from the QC collections
    and put all the QC functions within a single dictionary.

    :param package: package (name or actual module)
    :rtype: dict[str, types.ModuleType]
    """
    if isinstance(package, str):
        package = importlib.import_module(package)
    qcs = {}
    for loader, name, is_pkg in pkgutil.walk_packages(package.__path__):
        full_name = package.__name__ + '.' + name
        qc_collection = importlib.import_module(full_name)
        qc_attibutes = dir(qc_collection)
        for qc_attribute in qc_attibutes:
            if bool(re.match(r"^qc\d{1,3}$", qc_attribute)):
                qcs[qc_attribute] = getattr(qc_collection, qc_attribute)
    return qcs
