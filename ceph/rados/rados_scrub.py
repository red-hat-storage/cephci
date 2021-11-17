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

import logging
from collections import defaultdict

from ceph.rados.core_workflows import RadosOrchestrator

log = logging.getLogger(__name__)


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

        Returns: 0 or 1
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

    def verify_scrub(self, before_scrub_data, after_scrub_data):
        """
        Used to validate the scrubbing done or not

        Args:
            1.before_scrub_data - It is dictionary which conatin the
              pgId and last scrub time data before scrubbing.
            2.after_scrub_data - It is a dictionary that conatins the
              PDId and lst surb time data after scrubbing.
        Return: 0 or 1
        """
        before_scrub_log = dict(
            zip(before_scrub_data["pgid"], before_scrub_data["last_scrub_stamp"])
        )
        after_scrub_log = dict(
            zip(after_scrub_data["pgid"], after_scrub_data["last_scrub_stamp"])
        )
        number_of_pgs = len(before_scrub_data.keys())
        count_pg = 0
        for key in before_scrub_log.keys():
            if key in after_scrub_log:
                if before_scrub_log[key] != after_scrub_log[key]:
                    count_pg = count_pg + 1
                else:
                    count_pg = count_pg - 1
        if count_pg < round(number_of_pgs / 3):
            return 1
        return 0
