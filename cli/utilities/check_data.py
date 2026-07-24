from cli.rbd.rbd import Rbd
from utility.log import Log

log = Log(__name__)


is_list_equal = lambda x: all(i == x[0] for i in x)


def get_md5sum_rbd_image(**kw):
    """
    Get md5sum of an RBD image.
    kw: {
        "image_spec": <pool/image> for which md5sum needs to be fetched,
        "file_path": <path/filename> to which image is exported/mounted to fetch md5sum,
        "rbd": <rbd_object>,
        "client": <client_node>
    }
    """
    rbd = kw.get("rbd", Rbd(kw.get("client")))
    if kw.get("image_spec"):
        export_spec = {
            "source-image-or-snap-spec": kw.get("image_spec"),
            "path-name": kw.get("file_path"),
        }
        if kw.get("rbd").export(**export_spec):
            log.error(f"Export failed for image {kw.get('image_spec')}")
            return None
    elif kw.get("file_path"):
        out = rbd.execute(cmd="md5sum {}".format(kw.get("file_path")))
        log.info(f"Output of md5 sum command: {out}")
        return out.split()[0]
    else:
        log.error("Not enough arguments to fetch md5 sum")
        return None


def check_data_integrity_rbd_image(**kw):
    """
    Verifies whether the md5sum of the images given in arguments is equal

    Args:
        kw: {
            "images":[
                {
                    "image_spec": <>,
                    "file_path": <>,
                    "rbd":<rbd_object>,
                    "client":<client_node>
                },
                {
                    "image_spec": <>,
                    "file_path":<>,
                    "rbd":<rbd_object>,
                    "client":<client_node>
                }
            ]
        }
    """
    if len(kw.get("images")) < 2:
        log.error("Provided number of images is not enough for comparison")
        return 1

    md5_sums = list()
    for image_spec in kw.get("images"):
        md5_sum = get_md5sum_rbd_image(**image_spec)
        if not md5_sum:
            log.error("Error while fetching md5sum")
            return 1
        md5_sums.append(md5_sum)

    if is_list_equal(md5_sums):
        log.info("md5 sums of all the images specified are equal")
        return 0
    else:
        log.error("md5 sums for the given images don't match")
        log.error(f"The md5sum values retrieved are : {','.join(md5_sums)}")
        return 1
