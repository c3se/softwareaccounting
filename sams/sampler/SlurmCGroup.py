"""
Fetches Metrics from Slurm CGroup command

SAMS Software accounting
Copyright (C) 2018-2021  Swedish National Infrastructure for Computing (SNIC)

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; If not, see <http://www.gnu.org/licenses/>.



Config options:

sams.sampler.SlurmCGroup:
    # in seconds
    sampler_interval: 100

    cgroup_base: /cgroup

Output:
{
    cpus: 0,
    memory_usage: 0,
    memory_limit: 0,
    memory_max_usage: 0
}
"""

import logging
import os
import re

import sams.base

logger = logging.getLogger(__name__)


class Sampler(sams.base.Sampler):
    def __init__(self, id, outQueue, config):
        super(Sampler, self).__init__(id, outQueue, config)
        self.processes = {}
        self.cgroup = None
        self.cgroup_base = self.config.get([self.id, "cgroup_base"], "/cgroup")

    def do_sample(self):
        return self._get_cgroup()

    def sample(self):
        logger.debug("sample()")

        cpus = self._cpucount(self.read_cgroup("cpuset", "cpuset.cpus"))
        memory_usage = self.read_cgroup("memory", "memory.usage_in_bytes")
        memory_limit = self.read_cgroup("memory", "memory.limit_in_bytes")
        memory_max_usage = self.read_cgroup("memory", "memory.max_usage_in_bytes")
        memory_usage_and_swap = self.read_cgroup(
            "memory", "memory.memsw.usage_in_bytes"
        )

        entry = {
            "cpus": cpus,
            "memory_usage": memory_usage,
            "memory_limit": memory_limit,
            "memory_max_usage": memory_max_usage,
            "memory_swap": str(int(memory_usage_and_swap) - int(memory_usage)),
        }
        self._most_recent_sample = [self._storage_wrapping(entry)]
        self.store(entry)

    def _get_cgroup(self):
        """Get the cgroup base path for the slurm job"""
        if self.cgroup:
            return True
        for pid in self.pids:
            try:
                with open("/proc/%d/cpuset" % pid, "r") as file:
                    cpuset = file.readline()
                    m = re.search(r"^/(slurm/uid_\d+/job_\d+)/", cpuset)
                    if m:
                        self.cgroup = m.group(1)
                        return True
            except Exception as e:
                logger.debug("Failed to fetch cpuset for pid: %d", self.pids[0])
                logger.debug(e)
        return False

    @classmethod
    def _cpucount(cls, count):
        """Calculate number of cpus from a "N,N-N"-structure"""
        cpu_count = 0
        for c in count.split(","):
            m = re.search(r"^(\d+)-(\d+)$", c)
            if m:
                cpu_count += int(m.group(2)) - int(m.group(1)) + 1
            m = re.search(r"^(\d+)$", c)
            if m:
                cpu_count += 1
        return cpu_count

    def read_cgroup(self, type, id):
        try:
            with open(
                os.path.join(self.cgroup_base, type, self.cgroup, id), "r"
            ) as file:
                return file.readline().strip()
        except IOError as err:
            logger.debug(
                "Failed to open %s for reading",
                os.path.join(self.cgroup_base, type, self.cgroup, id),
            )
            logger.debug(err)
            return ""

    @classmethod
    def final_data(cls):
        return {}
