"""
Fetches the path and cpu usage of the running processes.

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

sams.sampler.Software:
    # in seconds
    sampler_interval: 100

    # Map current running execs into softwares for live reporting
    # software_mapper: sams.software.Regexp

Output:
Every sample:
{
    current: {
        user: 0,
        system: 0
    }
}
summary:
{
    execs: {
        PATH: {
            user: 0,
            system: 0,
        },
        PATH2: {
            user: 0,
            system 0,
        },
    },
    start_time: 0,
    end_time: 1,
}
"""
import logging
import os
import re
import time

import sams.base
import sams.core

logger = logging.getLogger(__name__)


class Process:
    def __init__(self, pid, jobid):
        self.pid = pid
        self.tasks = {}
        self.clock_tics = os.sysconf(os.sysconf_names["SC_CLK_TCK"])
        self.starttime = time.time()
        self.ignore = False
        self.done = False
        self.uptime = None
        self.updated = None

        try:
            self.exe = os.readlink("/proc/%d/exe" % self.pid)
            logger.debug("Pid: %d (JobId: %d) has exe: %s", pid, jobid, self.exe)
        except Exception:
            logger.debug(
                "Pid: %d (JobId: %d) has no exe or pid has disapeard", pid, jobid
            )
            self.ignore = True
            return

    def _parse_stat(self, stat):
        """Parse the relevant content from /proc/***/stat"""

        m = re.search(r"^\d+ \(.*\) [RSDZTyEXxKWPI] (.*)", stat)
        stats = m.group(1).split(r" ")
        return {
            "user": float(stats[14 - 4]) / self.clock_tics,  # User CPU time in s.
            "system": float(stats[15 - 4]) / self.clock_tics,  # System CPU time in s.
        }

    def update(self, uptime):
        """Update information about pids"""

        if self.done:
            logger.debug("Pid: %d is done", self.pid)
            return

        logger.debug("Update pid: %d", self.pid)

        self.uptime = uptime

        try:
            tasks = filter(
                lambda f: re.match(r"^\d+$", f), os.listdir("/proc/%d/task" % self.pid)
            )
            tasks = map(int, tasks)
        except Exception:
            logger.debug(
                "Failed to read /proc/%d/task, most likely due to process ending",
                self.pid,
            )
            self.done = True
            return

        for task in tasks:
            try:
                with open("/proc/%d/task/%d/stat" % (self.pid, task)) as f:
                    stat = f.read()
                    stats = self._parse_stat(stat)
                    self.tasks[task] = {
                        "user": stats["user"],
                        "system": stats["system"],
                    }
                    logger.debug(
                        "Task usage for pid: %d, task: %d, user: %f, system: %f",
                        self.pid,
                        task,
                        stats["user"],
                        stats["system"],
                    )

            except Exception:
                logger.debug("Ignore missing task for pid: %d", self.pid)

        self.updated = time.time()

    def aggregate(self):
        """Return the aggregated information for all tasks"""
        return {
            "starttime": self.starttime,
            "exe": self.exe,
            "user": sum(t["user"] for t in self.tasks.values()),
            "system": sum(t["system"] for t in self.tasks.values()),
        }


class Sampler(sams.base.Sampler):
    def __init__(self, id, outQueue, config):
        super(Sampler, self).__init__(id, outQueue, config)
        self.processes = {}
        self.create_time = time.time()
        self.last_sample_time = self.create_time
        self.last_total = {"user": 0, "system": 0}
        self.software_mapper = None
        self.metrics_to_average = self.config.get(
            [self.id, "metrics_to_average"],
            ["system", "user"])
        self._average_values = {k: 0 for k in self.metrics_to_average}
        self._last_averaged_values = {k: 0 for k in self.metrics_to_average}

        software_mapper = self.config.get([id, "software_mapper"], None)
        if software_mapper is not None:
            logger.debug("Loading software_mapper: %s", software_mapper)
            try:
                Software = sams.core.ClassLoader.load(software_mapper, "Software")
                self.software_mapper = Software(software_mapper, config)
            except Exception as e:
                logger.error("Failed to initialize: %s", software_mapper)
                logger.exception(e)

    def map_software(self, aggr):
        output = {}
        if not self.software_mapper:
            logger.debug("No software_mapper loaded...")
            return output
        for exe, data in aggr.items():
            try:
                sw = self.software_mapper.get(exe)
                if sw["ignore"]:
                    continue
                if sw["software"] not in output:
                    output[sw["software"]] = {"user": 0, "system": 0}
                output[sw["software"]]["user"] += data["user"]
                output[sw["software"]]["system"] += data["system"]
            except Exception as e:
                logger.debug(e)
        return output

    def sample(self):
        logger.debug("sample()")

        with open("/proc/uptime", "r") as f:
            uptime = float(f.readline().split()[0])

        for pid in self.pids:
            logger.debug("evaluate pid: %d", pid)
            if pid not in self.processes.keys():
                logger.debug("Create new instance of Process for pid: %d", pid)
                self.processes[pid] = Process(pid, self.jobid)
            self.processes[pid].update(uptime)

        # Send information about current usage
        aggr, total = self._aggregate()
        sample_time = time.time()
        time_diff = sample_time - self.last_sample_time
        entry = {
            "current": {
                "software": self.map_software(aggr),
                "total_user": total["user"],
                "total_system": total["system"],
                "user": (total["user"] - self.last_total["user"])
                / time_diff,
                "system": (total["system"] - self.last_total["system"])
                / time_diff,
                }
            }
        if sample_time - self.create_time < self.sample_interval / 2:
            entry["current"]["user"] = 0
            entry["current"]["system"] = 0
        self.compute_sample_averages(entry["current"])
        self._most_recent_sample = [self._storage_wrapping(entry)]
        self.store(entry)
        self.last_total = total
        self.last_sample_time = sample_time

    def compute_sample_averages(self, data):
        """ Computes averages of selected measurements by
        means of trapezoidal quadrature, approximating
        that the time this function is called is the actual
        time of sampling. This is not completely correct but simplifies
        the implementation.
        """
        sample_time = time.time()
        elapsed_time = sample_time - self.last_sample_time
        total_elapsed_time = sample_time - self.create_time
        for key, item in data.items():
            if key in self.metrics_to_average:
                # Trapezoidal quadrature
                weighted_item = (
                        0.5 * (float(item) + float(self._last_averaged_values[key])) * elapsed_time)
                self._last_averaged_values[key] = item
                previous_integral = self._average_values[key] * (total_elapsed_time - elapsed_time)
                new_integral = previous_integral + weighted_item
                self._average_values[key] = new_integral / total_elapsed_time

        for key, item in self._average_values.items():
            data[key + '_average'] = item
        data['elapsed_time'] = total_elapsed_time

    def last_updated(self):
        procs = list(filter(lambda p: not p.ignore, self.processes.values()))
        if not procs:
            return self.create_time
        return int(max(p.updated for p in procs))

    def start_time(self):
        procs = list(filter(lambda p: not p.ignore, self.processes.values()))
        if not procs:
            return 0
        return int(min(p.starttime for p in procs))

    def _aggregate(self):
        aggr = {}
        total = {"user": 0.0, "system": 0.0}
        for a in [
            p.aggregate()
            for p in filter(lambda p: not p.ignore, self.processes.values())
        ]:
            logger.debug(
                "_aggregate: exe: %s, user: %f, system: %f",
                a["exe"],
                a["user"],
                a["system"],
            )
            exe = a["exe"]
            if exe not in aggr:
                aggr[exe] = {"user": 0.0, "system": 0.0}
            aggr[exe]["user"] += a["user"]
            aggr[exe]["system"] += a["system"]
            total["user"] += a["user"]
            total["system"] += a["system"]
        return aggr, total

    def final_data(self):
        logger.debug("%s final_data", self.id)
        aggr, _ = self._aggregate()
        return {
            "execs": aggr,
            "start_time": self.start_time(),
            "end_time": self.last_updated(),
        }
