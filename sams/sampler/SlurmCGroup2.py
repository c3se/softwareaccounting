"""
Fetches Metrics from Slurm CGroup command for CGroups version 2.

For license information see sams.sampler.SlurmCGroup.

Config options:

sams.sampler.SlurmCGroup2:
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
from .SlurmCGroup import Sampler as BaseCGroupSampler

logger = logging.getLogger(__name__)


class Sampler(BaseCGroupSampler):
    def sample(self):
        logger.debug("sample()")

        cpus = self._cpucount(self.read_cgroup("cpuset.cpus"))
        user_cpu_usec = self.read_cgroup("cpu.stat", metric_name="user_usec")
        memory_usage = self.read_cgroup("memory.current")
        memory_limit = self.read_cgroup("memory.high")
        memory_max_usage = self.read_cgroup("memory.max")
        memory_usage_and_swap = self.read_cgroup(
            "memory.swap.current")

        entry = {
            "cpus": cpus,
            "user_cpu_usec": user_cpu_usec,
            "memory_usage": memory_usage,
            "memory_limit": memory_limit,
            "memory_max_usage": memory_max_usage,
            "memory_swap": str(int(memory_usage_and_swap) - int(memory_usage)),
        }
        self.compute_sample_averages(entry)
        self._most_recent_sample = [self._storage_wrapping(entry)]
        self.store(entry)

    @staticmethod
    def _get_cgroup_regex():
        return r"^/(system.slice/slurmstepd.scope/job_\d+)/"

    def _get_cgroup_item_path(self, value):
        """
        Version-specific parsing function. We assume the number
        of arguments passed by read_cgroup is correct to trigger
        any errors early.
        """
        return os.path.join(self.cgroup_base,
                            self.cgroup, value)
