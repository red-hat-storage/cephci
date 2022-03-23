
# Classes(based on Operations) for RBD
# snapshots   -- create, list, delete, rollback, purge, protect, clone, unprotect, flatten, list children
# basic --- remove, list, create, resize, info, restore
# mirroring  -- enable, disble, mirror-snapshots, promotion&demotion, status
# migration -- execute, migrate, abort


#test_config ---> workflowlayer(we combine various SDK layer operations) --> rbd_utils made into modular based on operation --> call specific SDK

from ceph.parallel import parallel
from lib.rbd.rbd import Rbd
import yaml


def merge_dicts(source, destination):
    pass


class TestRbd():

    @staticmethod
    def run(**kw):
        try:
            # Step1: merge suite and test config yamls
            merge_dicts(source, destination)
            # Step2: Get class and method names for an entrypoint to trigger
            for step in test_conf.steps:
                if step.get("operation") == "shell":
                    Rbd.shell()
                else:
                    instance = step["class"]()
                    # maintain dictionary to map to classes based on service
                    # instantiate class 
                    # method = getattr(dictionarykey, operation)
                    #  method()
                    method = getattr(instance, step["method"])
            #Step3: if parallel is set to True, use spawn, else execute testmodule steps sequentially
                    if test_conf.parallel:
                        with parallel() as p:
                            p.spawn(
                                method(step["args"])
                            )
                    else:
                        method(step["args"])

                pass
        except Exception as e:
            print()
        finally:
            print()