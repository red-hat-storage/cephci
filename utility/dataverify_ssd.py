import os
from urllib.request import Request, urlopen

import rados
import rbd

word_url = (
    "https://svnweb.freebsd.org/csrg/share/dict/words?view=co&content-type=text/plain"
)
req = Request(word_url, headers={"User-Agent": "Mozilla/5.0"})
webpage = urlopen(req).read()
words = webpage.splitlines()
# create image first
cluster = rados.Rados(conffile="/etc/ceph/ceph.conf")
try:
    cluster.connect()
    pool_name = "test_io"
    image_name = "image_io"
    os.system(f"ceph osd pool create {pool_name} 128")
    os.system(f"ceph osd pool application enable {pool_name} rbd")
    os.system(f"rbd pool init -p {pool_name}")
    ioctx = cluster.open_ioctx(pool_name)
    try:
        rbd_inst = rbd.RBD()
        size = 4 * 1024**3  # 4 GiB
        rbd_inst.create(ioctx, image_name, size)
        image = rbd.Image(ioctx, image_name)
        try:
            # generate and verify if the words written to rbd block and read are
            # the same
            for word in words:
                word_len = len(word)
                image.write(word, 0)
                read_word = image.read(0, word_len)
                print("expected word: {} read word {}".format(word, read_word))
                if word == read_word:
                    continue
                print("Error occured: expected word {} got {}".format(word, read_word))

        finally:
            image.close()
    finally:
        ioctx.close()
finally:
    cluster.shutdown()
