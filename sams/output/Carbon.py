"""
Sends output from Samplers to carbon server

Config Options:

sams.output.Carbon:
    server: carbon.server.example.com
    port: 2003

    # Fetches the value from dict-value and put into dict-key
    # This can be used in the 'metrics' dict-value with %(key)s
    map:
        jobid: sams.sampler.Core/jobid
        node: sams.sampler.Core/node

    # Sets the value from dict-value and put into dict-key
    # This can be used in the 'metrics' dict-value with %(key)s
    static_map:
        cluster: kebnekaise

    # Metrics matching dict-key will be sent to carbon server as dict-value
    metrics:    
        '^sams.sampler.SlurmCGroup/(?P<metric>\S+)$' : 'sa/%(cluster)s/%(jobid)s/%(node)s/%(metric)s'

"""
import sams.base
import time
import socket
import re

import logging
logger = logging.getLogger(__name__)

class Output(sams.base.Output):
    """ File output Class """

    def __init__(self,id,config):
        super(Output,self).__init__(id,config)
        self.static_map = self.config.get([self.id,"static_map"],{})
        self.map = self.config.get([self.id,"map"],{})
        self.metrics = self.config.get([self.id,"metrics"],{})
        self.server = self.config.get([self.id,"server"],'localhost')
        self.port = self.config.get([self.id,"port"],2003)
        self.data = {}

        # UDP Socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def dict2str(self,dct,base="",delim="/"):   
        out = []
        for key in dct.keys():
            nb = "/".join([base,key])
            if key in dct and type(dct[key]) is dict:
                out = out + self.dict2str(dct[key],base=nb)
            else:
                out = out + [{'match': nb, 'value':dct[key]}]
        return out

    def safe_metric(self, dct, keys):        
        for key in keys:
            if key in dct:
                dct = dct[key]
            else:
                return None
        return dct

    def store(self,data):
        logger.debug("store: %s" % data)        
        for k,v in data.items():
            self.data[k] = v

        flatdict = self.dict2str(data)
        for d in flatdict:
            for metric,destination in self.metrics.items():                
                reg = re.compile(metric)
                m = reg.match(d['match'])
                if m:
                    di = m.groupdict()
                    self.send(d['value'],destination,di)


    def send(self,value,destination,di):
        d = self.static_map.copy()
        d.update(di)
        for k,v in self.map.items():
            m = self.safe_metric(self.data,v.split('/'))
            if not v:                
                return
            d[k] = m

        dest = destination % d
        message = "%s %s %d\n" % (dest,value,int(time.time()))
        logger.debug("Sending: %s to %s:%s" % (message,self.server,self.port))
        self.sock.sendto(str.encode(message), (self.server, self.port))
        
    def write(self):
        pass

