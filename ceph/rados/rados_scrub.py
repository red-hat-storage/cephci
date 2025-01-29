"""
This module contains the methods required for scrubbing.

1.To set the parameters for scrubbing initially required the
  cluster time and day details.get_cluster_date method provides
  the details

2.set_osd_configuration method  used to set the configuration
  parameters on the cluster.

3.get_osd_configuration  method is used to get the configured parameters
  on the cluster.

  NOTE: With set_osd_configuration & get_osd_configuration methods can
       use to set the get the any OSD configuration parameters.

4. get_pg_dump  method is used to get the pg dump details from the cluster

5. verify_scrub  method used for the verification of scheduled scrub
   happened or not.
"""

import datetime
import json
from collections import defaultdict

from ceph.rados.core_workflows import RadosOrchestrator
from utility.log import Log

log = Log(__name__)


class RadosScrubber(RadosOrchestrator):
    def get_osd_configuration(self, param):
        """
        Used to get the osd parameter value
        Args:
            params : Parameter to get the value

        Returns : parameter value
        """

        log.info(f'{"Getting  osd configurations"}')
        cmd = f"ceph config get osd {param}"
        out = super().run_ceph_command(cmd=cmd)
        return out

    def set_osd_configuration(self, param, value):
        """
        Used to set the configuration parametrs to the OSD's.
        Args:
            params : Parameters to be set for OSD's

        Returns: 0  for success or 1 for failure
        """
        cmd = f"ceph config set  osd {param} {value}"
        out, err = self.node.shell([cmd])
        actual_value = self.get_osd_configuration(param)
        if actual_value == value:
            log.info(f"Configuration parameter {param} is set with {value} on osds")
            return 0
        else:
            return 1

    def get_pg_dump(self, *args):
        """
        Used to get the pg dump logs

        Args:
            1. If column value is null then return the pg dump output
            2. If column is any of the pgid,state,last_scrub_stamp,
               last_deep_scrub_stamp,last_active ..e.t.c value then
                returns the details about that column
        Return:
              dictionary

        """

        cmd = "ceph pg dump "
        column_dict = defaultdict(list)

        log.info(f'{"Getting  PG dump of the cluster"}')
        pgDump = super().run_ceph_command(cmd=cmd)
        if args:
            columns = list(args)
            for detail in pgDump["pg_map"]["pg_stats"]:
                for column in columns:
                    column_dict[column].append(detail[column])
            return column_dict
        else:
            return pgDump

    def verify_scrub_deepscrub(self, before_scrub_data, after_scrub_data, flag):
        """
        Used to validate the scrubbing and deep scrubbing done or not

        Args:
            1.before_scrub_data - It is dictionary which conatin the
              pgId and last scrub time data before scrubbing.
            2.after_scrub_data - It is a dictionary that conatins the
              PDId and lst surb time data after scrubbing.
            3.flag - scrub or deep-scrub
        Return: 0 for Pass or 1 for Failure
        """
        if flag == "scrub":
            stamp = "last_scrub_stamp"
        else:
            stamp = "last_deep_scrub_stamp"
        before_scrub_log = dict(
            zip(before_scrub_data["pgid"], before_scrub_data[stamp])
        )
        after_scrub_log = dict(zip(after_scrub_data["pgid"], after_scrub_data[stamp]))

        number_of_pgs = len(before_scrub_log.keys())
        count_pg = 0
        for key in before_scrub_log.keys():
            if key in after_scrub_log:
                if before_scrub_log[key] != after_scrub_log[key]:
                    count_pg = count_pg + 1
        if count_pg < round(number_of_pgs * 0.5):
            return 1
        return 0

    def add_begin_end_hours(self, begin_hours=0, end_hours=0):
        """
        Used to get the begin and end hours from the current time

        Args:
            1.begin_hours - Hours to add the current time
              to get start hour.
            2.end_hours - Hours to add the current time
              to get end hour.
        Return: cluster_begin_hour,cluster_begin_weekday,
                cluster_end_hour,cluster_end_weekday are
                starting and ending hours and weekdays.
        """
        # get current time of cluster
        current_time = self.get_cluster_date()
        yy, mm, dd, hh, day = current_time.split(":")
        date = datetime.datetime(int(yy), int(mm), int(dd), int(hh))

        # getting begin hour and begin weekday
        date += datetime.timedelta(hours=begin_hours)
        cluster_begin_hour = date.hour
        # In Linux weekday starts from Sunday(0) and in python Monday(0)
        # so adding one day
        date += datetime.timedelta(days=1)
        cluster_begin_weekday = date.weekday()

        # Reassign the date to current weekday
        date += datetime.timedelta(days=-1)

        # getting end hours and end weekday
        date += datetime.timedelta(hours=end_hours)
        cluster_end_hour = date.hour
        # In Linux weekday starts from Sunday(0) and in python Monday(0)
        date += datetime.timedelta(days=1)
        cluster_end_weekday = date.weekday()
        return (
            cluster_begin_hour,
            cluster_begin_weekday,
            cluster_end_hour,
            cluster_end_weekday,
        )

    def set_osd_flags(self, flag, value):
        """
        Command example: ceph osd set noscrub

        Used to set/unset the osd flags

        Args:
            1.flag: set or unset
              example: set|unset

            2.value - value of the falg
              example:pause|noup|nodown|noout|noin|nobackfill|
                norebalance|norecover|noscrub|nodeep-scrub|notieragent
        Returns: True/False
        """
        if value == "pause":
            chk_string = f"pauserd,pausewr is {flag}"
        else:
            chk_string = f"{value} is {flag}"

        cmd = f"ceph osd {flag} {value}"
        out, err = self.node.shell([cmd])
        if chk_string in out or chk_string in err:
            log.info(f"The OSD falg {value} is {flag} on the cluster.")
            return True
        log.error(f"Failed to {flag} OSD flag {value}")
        return False

    def get_dump_scrubs(self, osd_id):
        """
        Method is used to return the osd dump scrubs
         Args:
             osd_id: osd id number
        Returns:
              Dump scrub output in the json format.
        """
        base_cmd = f"cephadm shell --name osd.{osd_id} ceph daemon  osd.{osd_id}  dump_scrubs -f json 2>/dev/null"
        acting_osd_node = self.fetch_host_node(daemon_type="osd", daemon_id=osd_id)
        out_put_tuple = acting_osd_node.exec_command(sudo=True, cmd=base_cmd)
        out_put = tuple(x for x in out_put_tuple if x)
        return json.loads(out_put[0])

    def get_pg_dump_scrub(self, osd_id, pg_id):
        """
        Method is used to retrive the dump scrub of a pg
        Args:
             osd_id: osd id number
             pg_id: pg id
        Returns:
              Dump scrub output of a PG in the json format.

        """
        dump_scrub = self.get_dump_scrubs(osd_id)
        for pg_no in dump_scrub:
            if pg_no["pgid"] == pg_id:
                return pg_no
        log.error(f"The provided {pg_id} is not exist in dump scrub")
        return None
