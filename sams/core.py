
import yaml
try:
    from yaml import CLoader as YamlLoader, CDumper as YamlDumper
except ImportError:
    from yaml import YamlLoader, YamlDumper
import threading
try:
    import queue
except ImportError:
    import Queue as queue
import os

import logging
logger = logging.getLogger(__name__)

class Config:
    """ Config class, reads config_file.yaml """
    
    def __init__(self,config_file,extra={}):
        self._cfg = {}
        with open(config_file,"r") as file:
            self._cfg = yaml.load(file, Loader=YamlLoader)

        self._cfg = self._merge(extra,self._cfg)
    
    def _merge(self,source, destination):
        """ Merges two dicts """
        for key, value in source.items():
            if isinstance(value, dict):
                # get node or create one
                node = destination.setdefault(key, {})
                self._merge(value, node)
            else:
                destination[key] = value

        return destination

    def _get(self,cfg,items):
        """ Helper function to do the recursive fetch of values from cfg """
        item = items[0]
        if item in cfg:
            if len(items) > 1:
                return self._get(cfg[item],items[1:])
            else:
                return cfg[item]
        return None

    def get(self,items,default=None):
        """ get config value """
        value = self._get(self._cfg,items)
        if value is None:
            return default
        return value


class OneToN(threading.Thread):    
    """ Class that takes one Queue and forwards into N other queues """

    def __init__(self,id="OneToN"):
        super(OneToN,self).__init__()
        self.id = id
        
        self.inQueue = queue.Queue()
        self.outQueue = []
        self._lock = threading.Lock()
        
        self.start()

    def run(self):
        """ Thread that forwards data from inQueue to outQueue[] """
        while True:
            value = self.inQueue.get()
            logger.debug("%s received: %s" % (self.id,value))
            if value is None:
                break
            # Lock the outQueue so that is will not change during sending.
            self._lock.acquire()
            for q in self.outQueue:
                q.put(value)
            self._lock.release()
            self.inQueue.task_done()

        logger.debug("%s is waiting for outQueue to be done" % self.id)
        for q in self.outQueue:
            q.join()
        logger.debug("%s is done" % self.id)

    def addQueue(self,queue):
        """ Add an new output queue """
        # Lock the outQueue list so that it is consistent
        self._lock.acquire()
        self.outQueue.append(queue)
        self._lock.release()

    def put(self,value):
        """ Put value into the queue """
        logger.debug("%s put(%s)" % (self.id,value))
        self.inQueue.put(value)

    def exit(self):
        """ Enters an None value into the queue to exit """
        logger.debug("%s got exit message" % self.id)
        self.inQueue.put(None)
        self.join()

class ClassLoader:
    """ Static class that loads an class by name """

    @staticmethod
    def load(package,class_name):
        module = __import__(package,globals(),locals(),[class_name])
        new_class = getattr(module,class_name)
        return new_class

# The standard I/O file descriptors are redirected to /dev/null by default.
if hasattr(os, "devnull"):
   REDIRECT_TO = os.devnull
else:
   REDIRECT_TO = "/dev/null"

def createDaemon(umask = 0, workdir = "/", maxfds = 1024):
    """Detach a process from the controlling terminal and run it in the
    background as a daemon.
    """

    try:
        pid = os.fork()
    except OSError as e:
        raise Exception("%s [%d]" % (e.strerror, e.errno))

    if pid == 0:	# The first child.
        os.setsid()

        try:
            pid = os.fork()	# Fork a second child.
        except OSError as e:
            raise Exception("%s [%d]" % (e.strerror, e.errno))

        if pid == 0:	# The second child.
            os.chdir(workdir)
            os.umask(umask)
        else:
            os._exit(0)	# Exit parent (the first child) of the second child.
    else:
        os._exit(0)	# Exit parent of the first child.

    #return(0)

    import resource		# Resource usage information.
    maxfd = resource.getrlimit(resource.RLIMIT_NOFILE)[1]
    if maxfd == resource.RLIM_INFINITY:
        maxfd = maxfds

    # Iterate through and close all file descriptors.
    for fd in range(0, maxfd):
        try:
            os.close(fd)
        except OSError as o:	# ERROR, fd wasn't open to begin with (ignored)
            pass

    # This call to open is guaranteed to return the lowest file descriptor,
    # which will be 0 (stdin), since it was closed above.
    os.open(REDIRECT_TO, os.O_RDWR)    # standard input (0)

    # Duplicate standard input to standard output and standard
    # error.
    os.dup2(0, 1)            # standard output (1)
    os.dup2(0, 2)         # standard error (2)
    return(0)
