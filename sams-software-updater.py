#!/usr/bin/env python

"""
Software updater for SAMS Software accounting
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

        self.config = '/etc/sams/sams-software-updater.yaml'
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
        logformat = self.config.get(['core','logformat'],'%(asctime)s %(name)s:%(levelname)s %(message)s')
        if logfile:
            logging.basicConfig(filename=logfile, filemode='a',
                                format=logformat,level=loglevel_n)
        else:
            logging.basicConfig(format=logformat,level=loglevel_n) 

    def start(self):                
        updater = self.config.get(['core','updater'])
        try:
            Updater = sams.core.ClassLoader.load(updater,'Software')
            self.updater = Updater(updater,self.config)
        except Exception as e:
            logger.error("Failed to initialize: %s" % updater)
            logger.exception(e)
            exit(1)

        backend = self.config.get(['core','backend'])
        try:
            Backend = sams.core.ClassLoader.load(backend,'Backend')
            self.backend = Backend(backend,self.config)
        except Exception as e:
            logger.error("Failed to initialize: %s" % updater)
            logger.exception(e)
            exit(1)

        try:
            self.backend.update(self.updater)
        except Exception as e:
            logger.exception("Failed to update",e)

if __name__ == "__main__":
    Main().start()
