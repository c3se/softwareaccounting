"""
Pid finder using the slurm cgroup information in /proc

Config options:

sams.pidfinder.Slurm:
    # How long to wait (in seconds) after process was removed.
    grace_period: 600

"""
import sams.base
import time
import os
import re

import logging
logger = logging.getLogger(__name__)

class Pids(object):
    def __init__(self,pid,jobid):
        self._pid = pid
        self.jobid = jobid        
        self.injob = self.check_job()
        self.update()

    def update(self):
        self.last_seen = time.time()

    def check_job(self):
        try:
            with open('/proc/%d/cpuset' % self._pid) as file:
                cpuset = file.read()
                m = re.search(r'/job_([0-9]+)/',cpuset)
                if m:                  
                    if int(m.group(1)) == self.jobid:
                        return True
        except IOError as err:
            pass
        
        # This pid is not within the Slurm CGroup.
        return False

class PIDFinder(sams.base.PIDFinder):
    def __init__(self,id,jobid,config):
        super(PIDFinder,self).__init__(id,jobid,config)
        self.processes = {}
        self.procdir = '/proc'
        self.create_time = time.time()

    def find(self):
        pids = filter(lambda f: re.match('^\d+$',f),os.listdir(self.procdir))
        pids = map(lambda p: int(p),pids)

        new_pids = []

        for pid in pids:
            if not pid in self.processes.keys():
                self.processes[pid] = Pids(pid,self.jobid)
                if self.processes[pid].injob:
                    new_pids.append(pid)
            self.processes[pid].update()

        return new_pids

    def done(self):
        procs = list(filter(lambda p: p.injob,self.processes.values()))
        if not len(procs):
            return self.create_time < time.time()-self.config.get([self.id,'grace_period'],600)
        return max(p.last_seen for p in procs) < time.time()-self.config.get([self.id,'grace_period'],600)
