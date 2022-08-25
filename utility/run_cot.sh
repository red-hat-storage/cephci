#!/usr/bin/bash

set -eux -o pipefail

usage() {
cat <<EOF >&2

Usage: $0 -o <osdid> -p <pgid> -t <task> -f <fsid> [-i <imageurl>] [-s <0|1>]

Where:
    -o <osdid>    is the numeric ID of the OSD to run against.
                  ( required )

    -t <task>     is the task to be performed via the COT tool
                  ( required )

    -p <pgid>     is the Placement Group to perform provided task
                  ( required )

    -i <imageurl> is the image address to run the trim shell with.
                  ( optional, default is system default image )

    -s <startosd> is the option that when provided, does not restart the OSD
                  ( optional, default is 1, i.e restarts the OSD at the end of operation )

    -f <fsid> fsid of the cluster
                  ( required )

    NOTE:

    Though this script would be able to run most of the COT commands on a containerized ceph env, This is
    primarily written & was tested to run 3 Operations , i.e
    1. pg-log-inject-dups
    2. log
    3. trim-pg-log-dups

    Syntax :
    ceph-objectstore-tool --data-path path to osd [--pgid $PG_ID ][--op command] -> Supported
    ceph-objectstore-tool --data-path path to osd [ --op list $OBJECT_ID] -> No support yet

    DOC reference : https://docs.ceph.com/en/latest/man/8/ceph-objectstore-tool/
EOF
  exit 1
}

if [ $# -lt 4 ]; then
  echo
  echo "ERROR: Required parameters missing."
  usage
fi

## Defaults
osdid=""
operation=""
cephadmopts=""
maxtrim=500000
dups_tracked=999999999999
pgid=""
fsid=""
error=0
startosd=1

while getopts ":o:i:t:p:s:f:" o; do
  case "${o}" in
    i)
      cephadmopts="--image ${OPTARG}"
      ;;
    t)
      operation="${OPTARG}"
      ;;
    f)
      fsid="${OPTARG}"
      ;;
	o)
      if [ $(echo ${OPTARG} | egrep -c '^[0-9][0-9]*$') -eq 1 ]; then
        osdid=${OPTARG}
      else
        echo
        echo "ERROR: -o OSD must be numeric ID only."
	error=1
      fi
      ;;
    p)
      if [ $(echo ${OPTARG} | egrep -c "^[0-9][0-9]*\.[0-9a-f][0-9a-f]*$" ) -eq 1 ]; then
        pgid="${OPTARG}"
      else
        echo
        echo "ERROR: -p parameter must be a valid Placement Group ID format (Example: 1.a7)"
	error=1
      fi
      ;;
    s)
      if [ $(echo ${OPTARG} | egrep -c "^[0-1]$") -eq 1 ]; then
        startosd=${OPTARG}
      else
        echo
        echo "ERROR: -s paramter must be numeric only"
	error=1
      fi
      ;;
    *)
      echo
      echo "Unrecognized argument: ${o}"
      usage
      ;;
  esac
done

shift $((OPTIND-1))

if [ $error -gt 0 ]; then
  usage
fi

if [ -z "${osdid}" ]; then
  echo
  echo "ERROR: -o osdid required!"
  usage
fi

if [ -z "${fsid}" ]; then
  echo
  echo "ERROR: -f fsid required!"
  usage
fi

if [ -z ${operation} ]; then
  echo
  echo "ERROR: -t task required!"
  usage
fi

if [ -z "${pgid}" ]; then
  echo
  echo "ERROR: -p pgid required!"
  usage
fi

echo "Paramters:"
echo "  osdid=${osdid}"
echo "  cephadm image=${cephadmopts}"
echo "  pgid=${pgid}"
echo "  operation=${operation}"
echo " start osd = ${startosd}"
echo "Cluster fsid = ${fsid}"


mkdir -p /var/log/ceph/${fsid}/osd.${osdid} &>/dev/null

echo "INFO: stopping osd.${osdid}"
systemctl disable --now ceph-${fsid}@osd.${osdid}
cephadm unit --fsid ${fsid} --name osd.${osdid} stop

echo "Performing ${operation} on PG: ${pgid}"

if [ $operation == "pg-log-inject-dups" ]; then
  echo "INFO: Operation selected to inject dups"

  cat << EOF > /var/log/ceph/${fsid}/text.json
[{"reqid": "client.4177.0:0", "version": "111'999999999", "user_version": "0", "generate": "100", "return_code": "0"},]
EOF
  echo "created test file with 100 corrupt dups to be injected"
  cat << EOF > /var/log/ceph/${fsid}/osd.${osdid}/${operation}-${pgid}.${osdid}.sh
#!/usr/bin/bash

CEPH_ARGS='--no_mon_config --osd_pg_log_dups_tracked=${dups_tracked}' ceph-objectstore-tool \
 --data-path /var/lib/ceph/osd/ceph-${osdid} --op ${operation} --pgid ${pgid} --file /var/log/ceph/text.json \
  &> /var/log/ceph/osd.${osdid}/${operation}-${pgid}.${osdid}.log
EOF

else
  cat << EOF > /var/log/ceph/${fsid}/osd.${osdid}/${operation}-${pgid}.${osdid}.sh
#!/usr/bin/bash

CEPH_ARGS='--osd_pg_log_trim_max=${maxtrim}' ceph-objectstore-tool --data-path /var/lib/ceph/osd/ceph-${osdid} \
 --op ${operation} --pgid ${pgid} &> /var/log/ceph/osd.${osdid}/${operation}-${pgid}.${osdid}.log
EOF
fi

chmod 755 /var/log/ceph/${fsid}/osd.${osdid}/${operation}-${pgid}.${osdid}.sh

echo "INFO: Running COT operation ${operation} for osd.${osdid}"
cephadm $cephadmopts shell --fsid ${fsid} --name osd.${osdid} /var/log/ceph/osd.${osdid}/${operation}-${pgid}.${osdid}.sh

if [ $startosd -eq 1 ]; then
  echo "INFO: starting osd.${osdid}"
  systemctl enable ceph-${fsid}@osd.${osdid}
  cephadm unit --fsid ${fsid} --name osd.${osdid} start
fi

chmod 755 /var/log/ceph/${fsid}/osd.${osdid}/${operation}-${pgid}.${osdid}.log

echo "Completed COT operation ${operation} on osd :  osd.${osdid}"
