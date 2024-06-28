"""
Base classes

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
"""

import threading
import time

try:
    import queue
except ImportError:
    import Queue as queue

import logging

logger = logging.getLogger(__name__)


class PIDFinder:
    """ PIDFinder base class """

    def __init__(self, id, jobid, config):
        self.id = id
        self.jobid = jobid
        self.config = config

    # pylint: disable=no-self-use
    def find(self):
        raise Exception("Not implemented")
        # return []


class Sampler(threading.Thread):
    """ Sampler base class """

    def __init__(self, id, outQueue, config):
        super(Sampler, self).__init__()
        self.id = id
        self._most_recent_sample = None
        self.outQueue = outQueue
        self.config = config
        self.jobid = self.config.get(["options", "jobid"])
        self.pidQueue = queue.Queue()
        self.pids = []
        self.sampler_interval = self.config.get([self.id, "sampler_interval"], 60)

    def init(self):  # pylint: disable=no-self-use
        pass

    def run(self):
        try:
            self.init()
        except Exception:
            logger.exception("Failed to do self.init in %s", self.id)
        while True:
            try:
                pids = self.pidQueue.get(timeout=self.sampler_interval)
                if not pids:
                    self.pidQueue.task_done()
                    break
                logger.debug("Received new pids: %s", pids)
                self.pids.extend(pids)
                self.pidQueue.task_done()
            except queue.Empty:
                logger.debug("%s queue.Empty timeout", self.id)
            try:
                if self.do_sample():
                    self.sample()
            except Exception:
                logger.exception("Failed to do self.sample in %s", self.id)

        try:
            self.store(self.final_data(), "final")
        except Exception:
            logger.exception("Failed to do self.final_data in %s", self.id)
        self.outQueue.join()

    def _storage_wrapping(self, data, type="now"):
        """
        Convenience method for wrapping data into a dictionary used
        by several other methods.
        """
        return {"id": self.id, "data": data, "type": type} 

    def store(self, data, type="now"):
        self.outQueue.put(self._storage_wrapping(data, type))

    @property
    def most_recent_sample(self):
        """ The most recently sample or set of samples stored by a call
        to ``self.sample()``."""
        return self._most_recent_sample

    # this should be implemented in the real Sampler..
    # pylint: disable=no-self-use
    def sample(self):
        raise Exception("Not implemented")

    # this should be implemented in the real Sampler..
    # pylint: disable=no-self-use
    def final_data(self):
        raise Exception("Not implemented")

    def do_sample(self):
        return len(self.pids) > 0

    def exit(self):
        logger.debug("%s exit", self.id)
        self.pidQueue.put(None)


class Aggregator:
    """ Aggregator base class """

    def __init__(self, id, config):
        self.id = id
        self.config = config

    # pylint: disable=no-self-use
    def aggregate(self, data):
        raise Exception("Not implemented")


class Loader:
    """ Loader base class """

    def __init__(self, id, config):
        self.id = id
        self.config = config

    # pylint: disable=no-self-use
    def load(self):
        raise Exception("Not implemented")

    # pylint: disable=no-self-use
    def next(self):
        raise Exception("Not implemented")

    # pylint: disable=no-self-use
    def commit(self):
        raise Exception("Not implemented")


class Backend:
    """ Backend base class """

    def __init__(self, id, config):
        self.id = id
        self.config = config

    # pylint: disable=no-self-use
    def update(self, software):
        raise Exception("Not implemented")

    # pylint: disable=no-self-use
    def extract(self):
        raise Exception("Not implemented")


class Software:
    """ Software base class """

    def __init__(self, id, config):
        self.id = id
        self.config = config

    # pylint: disable=no-self-use
    def update(self):
        raise Exception("Not implemented")


class Output(threading.Thread):
    """ Output base class """

    def __init__(self, id, config):
        super(Output, self).__init__()
        self.id = id
        self.config = config

        self.dataQueue = queue.Queue()
        self.jobid = self.config.get(["options", "jobid"])

    def run(self):
        while True:
            data = self.dataQueue.get()
            if data is None:
                self.dataQueue.task_done()
                break
            try:
                self.store({data["id"]: data["data"]})
            except Exception:
                logger.exception("Failed to store")
            if "type" in data and data["type"] == "final":
                try:
                    self.final({data["id"]: data["data"]})
                except Exception:
                    logger.exception("Failed to do self.final in %s", self.id)
            self.dataQueue.task_done()

        for _ in range(int(self.config.get([self.id, "retry_count"], 3))):
            try:
                self.write()
                break
            except Exception:
                logger.exception("Failed to do self.write in %s", self.id)
            time.sleep(int(self.config.get([self.id, "retry_sleep"], 3)))

    # pylint: disable=no-self-use
    def store(self, data):
        raise Exception("Not implemented")

    def final(self, data):
        self.store(data)

    # pylint: disable=no-self-use
    def write(self):
        raise Exception("Not implemented")

    def exit(self):
        self.dataQueue.put(None)


class XMLWriter:
    """ XMLWriter base class """

    def __init__(self, id, config):
        self.id = id
        self.config = config

    # pylint: disable=no-self-use
    def write(self, data):
        raise Exception("Not implemented")
