"""
Fetches Metrics from Slurm command

Config options:

sams.sampler.Software:
    # in seconds
    sampler_interval: 100

    # path to scontrol command
    scontrol: /usr/local/bin/scontrol

Output:
Every sample:
{
    current: {
        user: 0,
        system: 0
    }
}
summary:
{
    execs: {
        PATH: {
            user: 0,
            system: 0,
        },
        PATH2: {
            user: 0,
            system 0,
        },
    },
    start_time: 0,
    end_time: 1,
}
"""
import sams.base
import time
import os
import re

import logging
logger = logging.getLogger(__name__)

class Process:
    def __init__(self,pid,jobid):
        self.pid = pid
        self.tasks = {}
        self.clock_tics = os.sysconf(os.sysconf_names['SC_CLK_TCK'])
        self.starttime = time.time()
        self.ignore = False
        self.done = False

        try:
            self.exe = os.readlink('/proc/%d/exe' % self.pid)
            logger.debug("Pid: %d (JobId: %d) has exe: %s",pid,jobid,self.exe)
        except OSError as err:
            logger.debug("Pid: %d (JobId: %d) has no exe or pid has disapeard",pid,jobid)
            self.ignore = True
            return
        except Exception as err:
            logger.debug("Pid: %d (JobId: %d) has no exe or pid has disapeard",pid,jobid)
            logger.info("No pids found 0 (%d), Exception: %s" % (self.pid, type(err)))
            self.ignore = True
            return

    def _parse_stat(self,stat):
        """ Parse the relevant content from /proc/***/stat """

        m = re.search(r'^\d+ \(.*\) [RSDZTyEXxKWP] (.*)',stat)
        stats = m.group(1).split(r' ')
        return {
            'user'  : float(stats[14-4])/self.clock_tics,   # User CPU time in s.
            'system': float(stats[15-4])/self.clock_tics,   # System CPU time in s.
        }

    def update(self,uptime):
        """ Update information about pids """

        if self.done:
            logger.debug("Pid: %d is done",self.pid)
            return

        logger.debug("Update pid: %d",self.pid)

        self.uptime = uptime

        try:
            tasks = filter(lambda f: re.match('^\d+$',f),os.listdir('/proc/%d/task' % self.pid))
            tasks = map(lambda t: int(t),tasks)
        except OSError as err:
            # Ignore if no tasks exists anymore (missing pid)
            logger.debug("No pids left/or no pids yet, considering done: %d" % self.pid)
            self.done = True
            return
        except Exception as err:
            # Ignore if no tasks exists anymore (missing pid)
            logger.debug("No pids left/or no pids yet, considering done: %d" % self.pid)
            logger.info("No pids found 1 (%d), Exception: %s" % (self.pid, type(err)))
            self.done = True
            return
        
        for task in tasks:
            try:
                with open('/proc/%d/task/%d/stat' % (self.pid,task)) as f:
                    stat = f.read()
                    stats = self._parse_stat(stat)               
                    self.tasks[task] = { 
                            'user': stats['user'],
                            'system': stats['system'],
                        }
                    logger.debug("Task usage for pid: %d, task: %d, user: %f, system: %f", 
                                    self.pid, task, stats['user'], stats['system'])
                    
            except IOError as err:
                logger.debug("Ignore missing task for pid: %d", self.pid)
                self.done = True
                # Ignore missing task (or pid)
                pass
            except Exception as err:
                logger.debug("Ignore missing task for pid: %d", self.pid)
                logger.info("No pids found 2 (%d, %d), exception: %s" % (self.pid, task, type(err)))
                self.done = True
                # Ignore missing task (or pid)
                pass

        self.updated = time.time()      

    def aggregate(self):
        """ Return the aggregated information for all tasks """
        return {
            'starttime': self.starttime,
            'exe': self.exe,
            'user': sum(t['user'] for t in self.tasks.values()),
            'system': sum(t['system'] for t in self.tasks.values()),
        }

class Sampler(sams.base.Sampler):
    def __init__(self,id,outQueue,config):
        super(Sampler,self).__init__(id,outQueue,config)
        self.processes = {}
        self.create_time = time.time()
        self.last_sample_time = None
        self.last_total = None

    def sample(self):
        logger.debug("sample()")

        with open('/proc/uptime', 'r') as f:
            uptime = float(f.readline().split()[0])
        
        for pid in self.pids:
            logger.debug("evaluate pid: %d",pid)
            if not pid in self.processes.keys():
                logger.debug("Create new instance of Process for pid: %d",pid)
                self.processes[pid] = Process(pid,self.jobid)
            self.processes[pid].update(uptime)

        # Send information about current usage 
        aggr,total = self._aggregate()
        if self.last_sample_time:            
            time_diff = time.time()-self.last_sample_time
            self.store({
                'current': {
                    'user': (total['user']-self.last_total['user'])/time_diff,
                    'system': (total['system']-self.last_total['system'])/time_diff
                }
            })

        self.last_total = total
        self.last_sample_time = time.time()

    def last_updated(self):
        procs = list(filter(lambda p: not p.ignore,self.processes.values()))
        if not len(procs):
            return self.create_time
        return int(max(p.updated for p in procs))

    def start_time(self):
        procs = list(filter(lambda p: not p.ignore,self.processes.values()))
        if not len(procs):
            return 0
        return int(min(p.starttime for p in procs))

    def _aggregate(self):
        aggr = {}
        total = { 'user': 0.0, 'system': 0.0 }
        for a in [p.aggregate() for p in filter(lambda p: not p.ignore, self.processes.values())]:
            logger.debug("_aggregate: exe: %s, user: %f, system: %f", a['exe'],a['user'],a['system'])
            exe = a['exe']
            if not exe in aggr:
                aggr[exe] = { 'user': 0.0, 'system': 0.0 }
            aggr[exe]['user'] += a['user']
            aggr[exe]['system'] += a['system']
            total['user'] += a['user']
            total['system'] += a['system']
        return aggr, total

    def final_data(self):
        logger.debug("%s final_data" % self.id)
        aggr,total = self._aggregate()
        return { 
            'execs': aggr,
            'start_time': self.start_time(),
            'end_time': self.last_updated(),
        }
