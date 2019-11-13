#!/usr/bin/env python

"""
Data Aggregator for SAMS Software accounting
"""

from __future__ import print_function

import getopt
import logging
import sys

import sams.core

logger = logging.getLogger(__name__)

class Options:
    def usage(self):
        print("usage....")

    def __init__(self,inargs):
        try:
            opts, args = getopt.getopt(inargs, "", ["help", "config=","logfile=","loglevel="])
        except getopt.GetoptError as err:
            # print help information and exit:
            print(str(err))  # will print something like "option -a not recognized"
            self.usage()
            sys.exit(2)

        self.config = '/etc/sams/sams-aggregator.yaml'
        self.logfile = None
        self.loglevel = None
        
        for o, a in opts:
            if o in "--config":
                self.config = a
            elif o in "--logfile":
                    self.logfile = a
            elif o in "--loglevel":
                self.loglevel = a
            else:
                assert False, "unhandled option %s = %s" % (o,a)
     
class Main:

    def __init__(self):
        self.options = Options(sys.argv[1:])
        self.config = sams.core.Config(self.options.config,{})

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

    def start(self):
        self.loaders = []
        self.aggregators = []

        for l in self.config.get(['core','loaders'],[]):
            try:
                Loader = sams.core.ClassLoader.load(l,'Loader')
                loader = Loader(l,self.config)
                self.loaders.append(loader)
            except Exception as e:
                logger.error("Failed to initialize: %s" % l)
                logger.error(e)
                exit(1)

        for a in self.config.get(['core','aggregators'],[]):
            try:
                Aggregator = sams.core.ClassLoader.load(a,'Aggregator')
                aggregator = Aggregator(a,self.config)
                self.aggregators.append(aggregator)
            except Exception as e:
                logger.error("Failed to initialize: %s" % a)
                logger.error(e)
                exit(1)

        logger.debug("Start loading %s",self.loaders)
        for l in self.loaders:
            l.load()
            while True:
                data = l.next()
                if not data:
                    break
                try:
                    logger.debug("Data: %s",data)
                    for a in self.aggregators:
                        a.aggregate(data)
                    l.commit()
                except Exception as e:
                    logger.error("Failed to do aggregation")
                    logger.exception(e)

                    # Cleanup of the aggregators.
                    for a in self.aggregators:
                        a.cleanup()
                    l.error()

        # Close down the aggregagors.
        for a in self.aggregators:
            a.close()


if __name__ == "__main__":
    Main().start()
