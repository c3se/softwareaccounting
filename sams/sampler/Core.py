"""
Saves core information about a job from the collector command line options.

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

sams.sampler.Core:
    # in seconds
    sampler_interval: 100

Output:
{
    jobid: 0,
    node: 0,
}
"""

import logging

import sams.base

logger = logging.getLogger(__name__)


class Sampler(sams.base.Sampler):
    def __init__(self, id, outQueue, config):
        super(Sampler, self).__init__(id, outQueue, config)
        self.core = {}

    def init(self):
        logger.debug("init()")
        self.core = {
            "jobid": self.config.get(["options", "jobid"]),
            "node": self.config.get(["options", "node"]),
        }
        self._most_recent_sample = [self._storage_wrapping(self.core)]
        self.store(self.core)

    def do_sample(self):
        return False

    def sample(self):
        logger.debug("sample()")

    def final_data(self):
        return self.core
