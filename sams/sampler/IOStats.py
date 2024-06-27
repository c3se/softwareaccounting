"""
Fetches Metrics from iostat command

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

sams.sampler.IOStats:
    # in seconds
    sampler_interval: 30

    # Path to iostat command
    iostat_command: /usr/bin/iostat

    # path(s) to devices to check (can use %(jobid)s and 'glob')
    iostat_devs: ['/dev/rootvg/slurm_%(jobid)s_*']

Output:
{
    'dm-20': {
        'wrqm_s': '0.00',
        'rkB_s': '0.00',
        'rrqm_s': '0.00',
        'util': '0.00',
        'avgqu-sz': '0.00',
        'r_await': '0.00',
        'wkB_s': '0.00',
        'await': '0.00',
        'r_s': '0.00',
        'w_await': '0.00',
        'w_s': '0.00',
        'svctm': '0.00',
        'avgrq-sz': '0.00'
    }
}

"""
import glob
import logging
import os
import subprocess
import threading

import sams.base

try:
    import queue
except ImportError:
    import Queue as queue

logger = logging.getLogger(__name__)

COMMAND = """%(iostat_command)s -xy -p %(devices)s %(interval)s"""


class IOStats(threading.Thread):
    def __init__(self, devices, t, command, device_map):
        super(IOStats, self).__init__()
        self.devices = devices
        self.device_map = device_map
        self.t = t
        self.command = command
        self.queue = queue.Queue()
        self.stop_event = threading.Event()

    def run(self):
        command = COMMAND % dict(
            iostat_command=self.command, devices=self.devices, interval=self.t
        )
        try:
            process = subprocess.Popen(command.split(" "), stdout=subprocess.PIPE)
            headers = []
            for line in iter(process.stdout.readline, b""):
                if self.stopped():
                    break
                items = line.decode("ascii").replace("\n", "").split()
                if not items:
                    continue

                if items[0] == "Device:":
                    headers = [x.replace("%", "").replace("/", "_") for x in items[1:]]

                if items[0].startswith("dm-"):
                    device = items.pop(0)
                    if ("/dev/%s" % device) in self.device_map:
                        device = self.device_map["/dev/%s" % device]
                    data = {}
                    for h in headers:
                        data[h] = items.pop(0)
                    self.queue.put({device: data})

        except Exception as e:
            logger.exception(e)
        process.kill()
        logger.debug("Exiting...")

    def stop(self):
        self.stop_event.set()

    def stopped(self):
        return self.stop_event.is_set()


class Sampler(sams.base.Sampler):
    def __init__(self, id, outQueue, config):
        super(Sampler, self).__init__(id, outQueue, config)
        self.processes = {}
        self.sampler_interval = self.config.get([self.id, "sampler_interval"], 60)
        self.iostat_command = self.config.get(
            [self.id, "iostat_command"], "/usr/bin/iostat"
        )
        self.iostat_devs = self.config.get([self.id, "iostat_devs"])
        self.jobid = self.config.get(["options", "jobid"], 0)

        if not self.iostat_devs:
            raise sams.base.SamplerException("iostat_devs not configured")

        self.job_iostat = None
        devices = []
        for dev in self.iostat_devs:
            devices += glob.glob(dev % dict(jobid=self.jobid))
        device_map = {}
        for dev in devices:
            rp = os.path.realpath(dev)
            device_map[rp] = dev
        if devices:
            self.job_iostat = IOStats(
                devices=",".join(devices),
                t=self.sampler_interval,
                command=self.iostat_command,
                device_map=device_map,
            )
            self.job_iostat.start()

    def do_sample(self):
        return self.job_iostat and not self.job_iostat.queue.empty()

    def sample(self):
        logger.debug("sample()")
        while not self.job_iostat.queue.empty():
            data = self.job_iostat.queue.get()
            logger.debug(data)
            self._most_recent_sample = [self._storage_wrapping(data)]
            self.store(data)

    def final_data(self):
        if self.job_iostat:
            self.job_iostat.stop()
            self.job_iostat.join()
        return {}
