#!/usr/bin/env python

"""
Data Aggregator for SAMS Software accounting
"""

from __future__ import print_function

import getopt
import logging
import os
import platform
import signal
import sys
import threading

import sams.core

logger = logging.getLogger(__name__)


class Options:
    def usage(self):
        print("usage....")

    def __init__(self,inargs):
        try:
            opts, args = getopt.getopt(inargs, "", ["help", "jobid=","config=",
                                                    "node=","logfile=","loglevel=",
                                                    "daemon","pidfile="])
        except getopt.GetoptError as err:
            # print help information and exit:
            print(str(err))  # will print something like "option -a not recognized"
            self.usage()
            sys.exit(2)

        self.node = platform.node().split(".")[0]
        self.jobid = None
        self.config = '/etc/sams/sams-collector.yaml'
        self.logfile = None
        self.loglevel = None
        self.daemon = False
        self.pidfile = None
        
        for o, a in opts:
            if o in "--node":
                self.node = a
            elif o in "--jobid":
                self.jobid = int(a)
            elif o in "--config":
                self.config = a
            elif o in "--logfile":
                self.logfile = a
            elif o in "--loglevel":
                self.loglevel = a
            elif o in "--pidfile":
                self.pidfile = a
            elif o in "--daemon":
                self.daemon = True
            else:
                assert False, "unhandled option %s = %s" % (o,a)

        if not self.jobid:
            assert False, "Missing option --jobid"
     
class Main:

    def __init__(self):
        self.options = Options(sys.argv[1:])
        self.config = sams.core.Config(self.options.config, {
            'options': {
                'jobid': self.options.jobid,
                'node': self.options.node,
            }
        })

        # Put process into background as daemon.
        # stdout/stderr will be closed.
        if self.options.daemon:
            try:
                sams.core.createDaemon()
            except Exception as e:
                logger.exception(e)
                exit(1)

        # Logging
        loglevel = self.options.loglevel
        if not loglevel:
            loglevel = self.config.get(['core','loglevel'],'ERROR')
        loglevel_n = getattr(logging, loglevel.upper(), None)
        if not isinstance(loglevel_n, int):
            raise ValueError('Invalid log level: %s' % loglevel)
        logfile = self.options.logfile
        if not logfile:
            logfile = self.config.get(['core','logfile'])
        if logfile:
            logfile = logfile % { 'jobid': self.options.jobid, 'node': self.options.node }           
        logformat = self.config.get(['core','logformat'],'%(asctime)s %(name)s:%(levelname)s %(message)s')
        if logfile:
            logging.basicConfig(filename=logfile, filemode='a',
                                format=logformat,level=loglevel_n)
        else:
            logging.basicConfig(format=logformat,level=loglevel_n)
        
        logger.debug("Loglevel: %s", loglevel)

        # Write pidfile.
        if self.options.pidfile:
            try:
                with open(self.options.pidfile,"w") as f:
                    f.write(str(os.getpid()))
            except Exception as e:
                logger.exception(e)
                exit(1)

        self.exit = threading.Event()
        # Trap signals
        signal.signal(signal.SIGHUP, self.sigHupHandler)
        signal.signal(signal.SIGINT, self.sigHupHandler)

    def sigHupHandler(self,signum,frame):
        self.exit.set()

    def cleanup(self):
        # Tell all samplers to exit
        for s in self.samplers:
            s.exit()

        # Wait for all samplers to finish
        for s in filter(lambda t: t.isAlive(),self.samplers):
            s.join()

        # Tell all output to exit
        for o in self.outputs:
            o.exit()

        # Wait for all outputs to finish
        for o in filter(lambda t: t.isAlive(),self.outputs):
            o.join()

        # exit queues
        self.pidQueue.exit()
        self.outQueue.exit()

    def start(self):
        self.samplers = []
        self.outputs = []
        self.pidQueue = sams.core.OneToN("pidQueue")
        self.outQueue = sams.core.OneToN("outQueue")

        for o in self.config.get(['core','outputs'],[]):
            logger.info("Load: %s",o)
            try:
                Output = sams.core.ClassLoader.load(o,'Output')
                output = Output(o,self.config)
                self.outputs.append(output)
                self.outQueue.addQueue(output.dataQueue)
                output.start()
            except Exception as e:
                logger.error("Failed to initialize: %s" % o)
                logger.error(e)
                self.cleanup()
                exit(1)

        for s in self.config.get(['core','samplers'],[]):
            logger.info("Load: %s",s)
            try:
                Sampler = sams.core.ClassLoader.load(s,'Sampler')
                sampler = Sampler(s,self.outQueue.inQueue,self.config)
                self.samplers.append(sampler)
                self.pidQueue.addQueue(sampler.pidQueue)
                sampler.start()
            except Exception as e:
                logger.error("Failed to initialize: %s" % s)
                logger.error(e)
                self.cleanup()
                exit(1)
 
        # load PIDFinder class
        PidFinder = sams.core.ClassLoader.load(self.config.get(['core','pid_finder']),'PIDFinder')        
        try:
            pid_finder = PidFinder(self.config.get(['core','pid_finder']),self.options.jobid,self.config)
        except Exception as e:
            logger.error("Failed to initialize: %s" % self.config.get(['core','pid_finder']))
            logger.error(e)
            self.cleanup()
            exit(1)

        while not self.exit.is_set() and not pid_finder.done():
            pids = pid_finder.find()
            if len(pids):
                self.pidQueue.put(pids)
            self.exit.wait(self.config.get(['core','pid_finder_update_interval'],30))

        self.cleanup()

if __name__ == "__main__":
    Main().start()
