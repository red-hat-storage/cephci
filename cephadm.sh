CEPHADM="sudo cephadm"

echo '# `ceph -s`'
echo '```'
$CEPHADM shell -- ceph -s
echo '```'

echo
echo '# `ceph orch ls`'
echo '```'
$CEPHADM shell -- ceph orch ls --format yaml
echo '```'

echo
echo '# `ceph orch ps`'
echo '```'
$CEPHADM shell -- ceph orch ps --format yaml
echo '```'

echo
echo '# `ceph orch host ls`'
echo '```'
$CEPHADM shell -- ceph orch host ls --format yaml
echo '```'

echo
echo '# `ceph orch device ls`'
echo '```'
$CEPHADM shell -- ceph orch device ls --format yaml
echo '```'


echo
echo '# `ceph log last cephadm`'
echo '```'
$CEPHADM shell -- ceph log last cephadm
echo '```'

echo '# `cephadm ls`'
echo '```'
$CEPHADM ls
echo '```'
