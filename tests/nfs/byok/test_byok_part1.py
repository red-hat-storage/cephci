from tests.nfs.byok.byok_certs import generate_self_signed_certificate
from utility.log import Log


log = Log(__name__)
def run(ceph_cluster, **kw):

    subject = {
        "common_name": "ceph-argo",
        "ip_address": "10.8.128.218"
    }
    rsa_key, cert, ca_cert = generate_self_signed_certificate(subject=subject)
    log.info(f"Generated RSA Key: {rsa_key}")
    log.info(f"Generated Certificate: {cert}")
    log.info(f"Generated CA Certificate: {ca_cert}")

    # create nsf ganesha
    nfs_cluster_dict_for_orch = {


        "cert": cert,
        "key": rsa_key,
        "ca_cert": ca_cert,
    }
