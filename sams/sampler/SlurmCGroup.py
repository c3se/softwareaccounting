"""
Fetches Metrics from Slurm CGroup command

Config options:

sams.sampler.SlurmCGroup:
    # in seconds
    sampler_interval: 100

    cgroup_base_path: /cgroup

Output:
{
    cpus: 0,
    memory_usage: 0,
    memory_limit: 0,
    memory_max_usage: 0
}
"""

import sams.base
import os
import re

import logging
logger = logging.getLogger(__name__)

class Sampler(sams.base.Sampler):
    def __init__(self,id,outQueue,config):
        super(Sampler,self).__init__(id,outQueue,config)
        self.processes = {}
        self.cgroup = None
        self.cgroup_base = self.config.get([self.id,"cgroup_base"],'/cgroup')

    def do_sample(self):
        return self._get_cgroup()

    def sample(self):
        logger.debug("sample()")

        cpus = self._cpucount(self.read_cgroup('cpuset','cpuset.cpus'))
        memory_usage = self.read_cgroup('memory','memory.usage_in_bytes')
        memory_limit = self.read_cgroup('memory','memory.limit_in_bytes')
        memory_max_usage = self.read_cgroup('memory','memory.max_usage_in_bytes')

        self.store({
                'cpus' : cpus,
                'memory_usage': memory_usage,
                'memory_limit': memory_limit,
                'memory_max_usage': memory_max_usage,
            })

    def _get_cgroup(self):
        """ Get the cgroup base path for the slurm job """
        if self.cgroup:
            return True
        for pid in self.pids:
            try:
                with open("/proc/%d/cpuset" % pid,"r") as file:
                    cpuset = file.readline()
                    m = re.search(r'^/(slurm/uid_\d+/job_\d+)/',cpuset)
                    if m:
                        self.cgroup = m.group(1)
                        return True
            except IOError as e:
                logger.debug("Failed to fetch cpuset for pid: %d", self.pids[0])
        return False

    def _cpucount(self,count):
        """ Calculate number of cpus from a "N,N-N"-structure """
        cpu_count = 0
        for c in count.split(","):
            m = re.search(r'^(\d+)-(\d+)$',c)
            if m:
                cpu_count += int(m.group(2))-int(m.group(1))+1
            m = re.search(r'^(\d+)$',c)
            if m:
                cpu_count += 1
        return cpu_count

    
    def read_cgroup(self,type,id):
        try:
            with open(os.path.join(self.cgroup_base,type,self.cgroup,id),"r") as file:
                return file.readline().strip()
        except IOError as err:
            logger.debug("Failed to open %s for reading",os.path.join(self.cgroup_base,type,self.cgroup,id))
            return ""

    def final_data(self):
        return {}
