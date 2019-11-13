"""
Fetches Metrics from Slurm command

Config options:

sams.sampler.SlurmInfo:
    # in seconds
    sampler_interval: 100

    # path to scontrol command
    scontrol: /usr/local/bin/scontrol

Output:
{
    account: "",
    cpus: 0,
    nodes: 0,
    username: "user",
    uid: 65535,
}
"""
import logging
import os
import re

import sams.base

logger = logging.getLogger(__name__)

COMMAND="%s show job %d -o"

class Sampler(sams.base.Sampler):
    data = {}

    def do_sample(self):
        if ( 'account' in self.data and 
                'cpus' in self.data and
                'nodes' in self.data and
                'starttime' in self.data and
                'username' in self.data and
                'uid' in self.data ):
            return False
        return True

    def init(self):
        self.sample()

    def sample(self):
        logger.debug("sample()")

        scontrol=self.config.get([self.id,'scontrol'],'/usr/local/bin/scontrol')
        jobid=self.config.get(['options','jobid'],0)

        command = COMMAND % (scontrol,jobid)

        try:
            process = os.popen(command)
            data = process.readlines()
        except Exception as e:
            logger.debug("Fail to run: %s, will try again in a while",command)
            # Try again next time :-)
            return

        data[0].strip()

        # Find account in string
        account = re.search(r'Account=([^ ]+)',data[0])
        if account:
            self.data['account'] = account.group(1)

        # Find username/uid in string\((\d+)\)
        userid = re.search(r'UserId=([^\(]+)\((\d+)\)',data[0])
        if userid:
            self.data['username'] = userid.group(1)
            self.data['uid'] = userid.group(2)

        # Find username/uid in string
        nodes = re.search(r'NumNodes=(\d+)',data[0])
        if nodes:
            self.data['nodes'] = nodes.group(1)

        # Find username/uid in string
        cpus = re.search(r'NumCPUs=(\d+)',data[0])
        if cpus:
            self.data['cpus'] = cpus.group(1)

        # Find StartTime
        starttime = re.search(r'StartTime=(\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d)',data[0])
        if starttime:
            self.data['starttime'] = starttime.group(1)

        if not self.do_sample():
            self.store(self.data)

    def final_data(self):
        return self.data
