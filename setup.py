"""
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

import os
from distutils.command.install import install
from distutils.command.install_data import install_data
from distutils.core import setup

import setuptools

from sams import __version__

# nasty global for relocation
RELOCATE = None


class InstallSams(install):
    def finalize_options(self):
        install.finalize_options(self)

        global RELOCATE
        RELOCATE = self.home


class InstallDataSams(install_data):
    # this class is used to filter out data files which should not be overwritten

    def finalize_options(self):
        install_data.finalize_options(self)

        # relocation
        if RELOCATE:
            print("relocating to %s" % RELOCATE)
            for (prefix, files) in reversed(self.data_files):
                if prefix.startswith("/"):
                    new_prefix = os.path.join(RELOCATE, prefix[1:])
                    self.data_files.remove((prefix, files))
                    self.data_files.append((new_prefix, files))

        # check that we don't overwrite /etc files
        for (prefix, files) in reversed(self.data_files):
            if prefix.startswith(os.path.join(RELOCATE or "/", "etc")):
                for basefile in files:
                    fn = os.path.join(prefix, os.path.basename(basefile))
                    if os.path.exists(fn):
                        print("Skipping installation of %s (already exists)" % fn)
                        files.remove(basefile)
            if not files:
                self.data_files.remove((prefix, []))


cmdclasses = {"install": InstallSams, "install_data": InstallDataSams}

setup(
    name="sams-software-accounting",
    version=__version__,
    description="SAMS Software Accounting",
    author="Magnus Jonsson",
    author_email="magnus@hpc2n.umu.se",
    url="http://www.hpc2n.umu.se/",
    packages=[
        "sams",
        "sams.aggregator",
        "sams.loader",
        "sams.output",
        "sams.pidfinder",
        "sams.sampler",
        "sams.backend",
        "sams.software",
        "sams.xmlwriter",
        "sams.listeners",
    ],
    scripts=[
        "sams-aggregator.py",
        "sams-collector.py",
        "sams-post-receiver.py",
        "sams-software-extractor.py",
        "sams-software-updater.py",
        "extras/sgas-sa-registrant/bin/sgas-sa-registrant",
    ],
    install_requires=[
        "Flask",
        "httplib2",
        "Twisted",
        "PyYAML",
    ],
    cmdclass=cmdclasses,
    data_files=[
        ("/etc/sams", ["sams-aggregator.yaml"]),
        ("/etc/sams", ["sams-collector.yaml"]),
        ("/etc/sams", ["sams-post-receiver.yaml"]),
        ("/etc/sams", ["sams-software-extractor.yaml"]),
        ("/etc/sams", ["sams-software-updater.yaml"]),
        ("/etc/sams", ["sams-post-receiver.yaml"]),
        ("/etc/sams", ["extras/sgas-sa-registrant/etc/sgas-sa-registrant.conf"]),
    ],
)
