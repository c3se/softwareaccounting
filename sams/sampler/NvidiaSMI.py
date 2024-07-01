"""
Fetches Metrics from nvidia-smi command

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

sams.sampler.NvidiaSMI:
    # in seconds
    sampler_interval: 100

    # Path to nvidia-smi command
    nvidia_smi_command: /usr/bin/nvidia-smi

    # Environment variable with indexs (","-separated) of GPUs
    gpu_index_environment: SLURM_JOB_GPUS

    # Metrics to collect. For list of available metrics: nvidia-smi --help-query-gpu
    nvidia_smi_metrics:
      - power.draw
      - power.limit
      - clocks.applications.memory
      - clocks.applications.graphics
      - clocks.current.graphics
      - clocks.current.sm
      - utilization.gpu
      - utilization.memory

Output:
{
    gpu_index: {
        power_draw: 0,
        power_limit: 0,
        clocks_applications_memory: 0,
        clocks_applications_graphics: 0,
        clocks_current_graphics: 0,
        clocks_current_sm: 0,
        utilization_gpu: 0,
        utilization_memory: 0
    }
}

"""
import logging
import os
import re
import subprocess
import threading
import time

import sams.base

logger = logging.getLogger(__name__)

try:
    import queue
except ImportError:
    import Queue as queue

COMMAND = """%s --query-gpu=index,%s --format=csv,nounits -l %d -i %s"""


class SMI(threading.Thread):
    def __init__(self, gpus, t, command, nvidia_smi_metrics):
        super(SMI, self).__init__()
        self.gpus = gpus
        self.t = t
        self.command = command
        self.queue = queue.Queue()
        self.stop_event = threading.Event()
        self.nvidia_smi_metrics = ",".join(
            [re.sub(r"[^a-z0-9_\.]+", "", m) for m in nvidia_smi_metrics]
        )

    def run(self):
        command = COMMAND % (
            self.command,
            self.nvidia_smi_metrics,
            self.t,
            ",".join(self.gpus),
        )
        try:
            process = subprocess.Popen(command.split(" "), stdout=subprocess.PIPE)
            head = process.stdout.readline()
            headers = map(
                lambda h: re.sub(r" \[[^\]]+\]$", "", h).replace(".", "_"),
                head.decode("ascii").replace("\n", "").split(", "),
            )
            headers = list(headers)
            for line in iter(process.stdout.readline, b""):
                if self.stopped():
                    break
                items = line.decode("ascii").replace("\n", "").split(", ")
                out = {}
                for h in headers:
                    out[h] = items[0]
                    items = items[1:]
                self.queue.put(out)
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
        self._start_time = time.time()
        self._last_sample_time = dict()
        self._average_values = dict()
        self._last_averaged_values = dict()
        self.processes = {}
        self.sampler_interval = self.config.get([self.id, "sampler_interval"], 60)
        self.gpu_index_environment = self.config.get(
            [self.id, "gpu_index_environment"], "SLURM_JOB_GPUS"
        )
        self.nvidia_smi_command = self.config.get(
            [self.id, "nvidia_smi_command"], "/usr/bin/nvidia-smi"
        )
        self.nvidia_smi_metrics = self.config.get(
            [self.id, "nvidia_smi_metrics"],
            [
                "power.draw",
                "power.limit",
                "clocks.applications.memory",
                "clocks.applications.graphics",
                "clocks.current.graphics",
                "clocks.current.sm",
                "utilization.gpu",
                "utilization.memory",
            ],
        )
        self.metrics_to_average = self.config.get(
            [self.id, "metrics_to_average"],
            [
                "power.draw",
                "utilization.gpu",
                "utilization.memory",
            ],
        )

        self.smi = None
        if self.gpu_index_environment in os.environ:
            self.gpustr = os.environ[self.gpu_index_environment]
            if self.gpustr:
                gpus = self.gpustr.split(",")
                self.smi = SMI(
                    gpus=gpus,
                    t=self.sampler_interval,
                    command=self.nvidia_smi_command,
                    nvidia_smi_metrics=self.nvidia_smi_metrics,
                )
                self.smi.start()

    def do_sample(self):
        return self.smi and not self.smi.queue.empty()

    def sample(self):
        logger.debug("sample()")
        most_recent_sample = []
        while not self.smi.queue.empty():
            data = self.smi.queue.get()
            logger.debug(data)
            index = data["index"]
            del data["index"]
            self.compute_sample_averages(data, index)
            entry = {index: data}
            most_recent_sample.append(self._storage_wrapping(entry))
            self.store(entry)
        self._most_recent_sample = most_recent_sample

    def compute_sample_averages(self, data, index):
        """ Computes averages of selected measurements by
        means of trapezoidal quadrature, approximating
        that the time this function is called is the actual
        time of sampling. This is not completely correct but simplifies
        the implementation.
        """
        sample_time = time.time()
        if index not in self._last_sample_time:
            # Keep it simple by approximating sampling time
            self._last_sample_time[index] = self._start_time
            self._average_values[index] = dict()
            self._last_averaged_values[index] = dict()
            for key in data:
                if key.replace('_', '.') in self.metrics_to_average:
                    # Initialize trapezoidal integral at 0.
                    self._average_values[index][key] = 0.
                    self._last_averaged_values[index][key] = 0.
        elapsed_time = sample_time - self._last_sample_time[index]
        total_elapsed_time = sample_time - self._start_time
        average_values = self._average_values[index]
        last_averaged_values = self._last_averaged_values[index]
        self._last_sample_time[index] = sample_time
        for key, item in data.items():
            if key.replace('_', '.') in self.metrics_to_average:
                # Trapezoidal quadrature
                weighted_item = (
                        0.5 * (float(item) + float(last_averaged_values[key])) * elapsed_time)
                last_averaged_values[key] = item
                previous_integral = average_values[key] * (total_elapsed_time - elapsed_time)
                new_integral = previous_integral + weighted_item
                average_values[key] = new_integral / total_elapsed_time

        for key, item in average_values.items():
            data[key + '_average'] = item
        data['elapsed_time'] = total_elapsed_time

    def final_data(self):
        if self.smi:
            self.smi.stop()
            self.smi.join()
        return {}
