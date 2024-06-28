"""
Fetches Metrics about a zfs volume

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
            entry = self._zfsstat.sample()
            self._most_recent_sample = [self._storage_wrapping(entry)]
            self.store(entry)

    @classmethod
    def final_data(cls):
        return {}
