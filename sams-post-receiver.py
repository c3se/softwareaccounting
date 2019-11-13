#!/usr/bin/env python

"""
Simple POST receiver for SAMS Software accounting
"""

from __future__ import print_function

import os
import sys
import getopt

from flask import Flask
from flask import request
from flask.views import MethodView

import sams.core

import logging
logger = logging.getLogger(__name__)

class Receiver(MethodView):
    def __init__(self,base_path,jobid_hash_size):
        super(Receiver,self).__init__()
        self.base_path = base_path
        self.jobid_hash_size = jobid_hash_size

    def post(self,jobid,filename):
               
        base_path = self.base_path

        if self.jobid_hash_size is not None:
            base_path = os.path.join(base_path,str(int(int(jobid) / int(self.jobid_hash_size))))

            if not os.path.isdir(base_path):
                try:
                    os.mkdir(base_path)
                except IOError as err:
                    # Handle possible raise from other process
                    if not os.path.isdir(base_path):
                        assert False, "Failed to mkdir '%s' " % base_path

        tfilename = ".%s" % filename
        try:
            with open(os.path.join(base_path,tfilename),"wb") as file:
                file.write(request.data)
            os.rename(os.path.join(base_path,tfilename),os.path.join(base_path,filename))
        except IOError as err:            
            logger.debug("Failed to write file")
            try:
                os.unlink(os.path.join(base_path,tfilename))
            except IOError as err:
                # Just log unlink errors
                logger.error("Failed to unlink tmp file")                
                pass
            raise Exception("Failed to write")
        return "OK"


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

        self.config = '/etc/sams/sams-post-receiver.yaml'
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
        app = Flask(__name__)
        view_func = Receiver.as_view('receiver', 
                                base_path=self.config.get(['core','base_path'],'/tmp'),
                                jobid_hash_size=self.config.get(['core','jobid_hash_size'])
                             )
        app.add_url_rule('/<int:jobid>/<filename>',view_func=view_func)
        app.run(host=self.config.get(['core','bind'],'127.0.0.1'), port=self.config.get(['core','port'],8080) )

if __name__ == "__main__":
    Main().start()
