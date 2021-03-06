"""
Fetches Metrics about a zfs volume

Config options:

sams.sampler.ZFSStats:
    # in seconds
    sampler_interval: 30

    # Volumes to check (can use "%(jobid)s")
    volumes: ['local/tmp.%(jobid)s']

    # ZFS command
    zfs_command: /sbin/zfs

Output:
{
    'local/tmp.12345': {
        free: 0,
        used: 0,
        size: 0
    }
}
"""

import logging
import subprocess

import sams.base

logger = logging.getLogger(__name__)


class ZFSStats:
    def __init__(self, volumes, zfs_command="/sbin/zfs"):
        self.volumes = volumes
        self.zfs_command = zfs_command

    def zfs_data(self, volume):
        process = subprocess.Popen(
            [self.zfs_command, "list", "-Hp", "-o", "used,avail", volume],
            stdout=subprocess.PIPE,
        )
        used, avail = process.stdout.readline().strip().split()
        return int(used), int(avail)

    def sample(self):
        ret = {}
        for v in self.volumes:
            try:
                (used, avail) = self.zfs_data(v)
                ret[v] = dict(size=avail + used, free=avail, used=used)
            except Exception as e:
                logger.error(e)
        return ret


class Sampler(sams.base.Sampler):
    def __init__(self, id, outQueue, config):
        super(Sampler, self).__init__(id, outQueue, config)
        self.processes = {}
        self.volumes = self.config.get([self.id, "volumes"])
        self.zfs_command = self.config.get([self.id, "zfs_command"], "/sbin/zfs")
        self.jobid = self.config.get(["options", "jobid"], 0)

        if not self.volumes:
            raise Exception("volumes not configured")

        volumes = [volume % dict(jobid=self.jobid) for volume in self.volumes]

        self.zfsstat = None
        if volumes:
            self.zfsstat = ZFSStats(volumes=volumes, zfs_command=self.zfs_command)

    def do_sample(self):
        if not self.zfsstat:
            return False
        return True

    def sample(self):
        logger.debug("sample()")
        if self.zfsstat:
            self.store(self.zfsstat.sample())

    def final_data(self):
        return {}
