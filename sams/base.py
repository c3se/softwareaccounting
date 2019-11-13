"""
Base classes
"""
import time

try:
    from yaml import CLoader as YamlLoader, CDumper as YamlDumper
except ImportError:
    from yaml import YamlLoader, YamlDumper
import threading
try:
    import queue
except ImportError:
    import Queue as queue


import logging
logger = logging.getLogger(__name__)

class PIDFinder(object):
    """ PIDFinder base class """

    def __init__(self,id,jobid,config):
        self.id = id
        self.jobid = jobid
        self.config = config

    def find(self):
        raise Exception("Not implemented")
        # return []

class Sampler(threading.Thread):
    """ Sampler base class """

    def __init__(self,id,outQueue,config):
        super(Sampler,self).__init__()
        self.id = id
        self.outQueue = outQueue
        self.config = config
        self.jobid = self.config.get(['options','jobid'])
        self.pidQueue = queue.Queue()
        self.pids = []

    def init(self):
        pass

    def run(self):
        try:
            self.init()
        except Exception as e:
            logger.exception("Failed to do self.init in %s" % self.id,e)
        while True:
            try:
                pids = self.pidQueue.get(timeout = self.config.get([self.id,"sampler_interval"],60))
                if not pids:
                    self.pidQueue.task_done()
                    break
                self.pids.extend(pids)
                self.pidQueue.task_done()
            except queue.Empty as e:
                logger.debug("%s queue.Empty timeout" % self.id)
                pass
            try:
                if self.do_sample():
                    self.sample()
            except Exception as e:
                logger.exception("Failed to do self.sample in %s" % self.id,e)
            
        try:
            self.store(self.final_data(),'final')
        except Exception as e:
            logger.exception("Failed to do self.final_data in %s" % self.id,e)
        self.outQueue.join()
    
    def store(self,data,type='now'):
        self.outQueue.put({
                'id': self.id,
                'data': data,
                'type': type
            })

    # this should be implemented in the real Sampler..
    def sample(self):
        raise Exception("Not implemented")

    def do_sample(self):
        return len(self.pids) > 0

    def exit(self):
        logger.debug("%s exit" % self.id)
        self.pidQueue.put(None)

class Aggregator(object):
    """ Aggregator base class """

    def __init__(self,id,config):
        self.id = id
        self.config = config

    def aggregate(self,data):
        raise Exception("Not implemented")

class Loader(object):
    """ Loader base class """

    def __init__(self,id,config):
        self.id = id
        self.config = config        

    def load(self):
        raise Exception("Not implemented")
    
    def next(self):
        raise Exception("Not implemented")

    def commit(self):
        raise Exception("Not implemented")

class Backend(object):
    """ Backend base class """

    def __init__(self,id,config):
        self.id = id
        self.config = config

    def update(self,updater):
        raise Exception("Not implemented")

class Software(object):
    """ Software base class """

    def __init__(self,id,config):
        self.id = id
        self.config = config

    def update(self):
        raise Exception("Not implemented")
        
class Output(threading.Thread):
    """ Output base class """
        
    def __init__(self,id,config):
        super(Output,self).__init__()
        self.id = id
        self.config = config

        self.dataQueue = queue.Queue()
        self.jobid = self.config.get(['core','jobid'])

    def run(self):
        while True:
            data = self.dataQueue.get()
            if data is None:
                self.dataQueue.task_done()
                break
            try:
                self.store({ data['id']: data['data'] })
            except:
                logger.exception("Failed to store",e)
            if 'type' in data and data['type'] == 'final':
                try:
                    self.final({ data['id']: data['data'] })
                except Exception as e:
                    logger.exception("Failed to do self.final in %s" % self.id,e)
            self.dataQueue.task_done()

        for t in range(int(self.config.get([self.id,'retry_count'],3))):
            try:
                self.write()
                break
            except Exception as e:
                logger.exception("Failed to do self.write in %s" % self.id,e)
            time.sleep(int(self.config.get([self.id,'retry_sleep'],3)))

    def store(self,data):
        raise Exception("Not implemented")

    def final(self,data):
        self.store(data)

    def write(self):
        raise Exception("Not implemented")

    def exit(self):
        self.dataQueue.put(None)
