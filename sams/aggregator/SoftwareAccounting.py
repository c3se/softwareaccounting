"""
Software accounting storage "backend" for updating the list of softwares

Config options:

sams.backend.SoftwareAccounting:
    # sqlite file pattern (regexp)
    file_pattern: 'sa-\d+.db'

    # Path to sqlite db files
    db_path: /data/softwareaccounting/CLUSTER/db

"""

import sqlite3
import os
import sams.base

import logging
logger = logging.getLogger(__name__)

""" Create tables unless exists """
TABLES = [
    ''' 
    CREATE TABLE IF NOT EXISTS projects (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        project         TEXT NOT NULL
    );
    ''',
    ''' 
    CREATE TABLE IF NOT EXISTS users (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        user            TEXT NOT NULL
    );
    ''',
    ''' 
    CREATE TABLE IF NOT EXISTS jobs (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        jobid           TEXT NOT NULL,
        user            INTEGER,
        project         INTEGER,        
        ncpus           INTEGER,
        start_time      INTEGER,
        end_time        INTEGER,
        user_time       REAL,
        system_time     REAL
    );
    ''',
    '''
    CREATE TABLE IF NOT EXISTS software (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        path            TEXT NOT NULL,
        software        TEXT,
        version         TEXT,
        versionstr      TEXT,
        user_provided   BOOLEAN
    );
    ''',
    '''
    CREATE INDEX IF NOT EXISTS software_path_idx on software(path);
    ''',
    '''
    CREATE TABLE IF NOT EXISTS node (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        node            TEXT NOT NULL
    );
    ''',
    '''
    CREATE INDEX IF NOT EXISTS node_node_idx on node(node);
    ''',
    '''
    CREATE TABLE IF NOT EXISTS command (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        jobid           INTEGER NOT NULL,
        node            INTEGER,
        software        INTEGER,
        start_time      INTEGER,
        end_time        INTEGER,
        user            REAL,
        sys             REAL,
        updated         INTEGER,
        FOREIGN KEY(node) REFERENCES node(node),
        FOREIGN KEY(software) REFERENCES software(software)
    );
    ''',
    '''
    CREATE INDEX IF NOT EXISTS command_jobid_node_software_idx on command(jobid,node,software);
    ''',
]

# Update/Insert SQL
INSERT_USER=''' insert or replace into users (id,user) values ((select ID from users where user = :user), :user); '''
INSERT_PROJECT=''' insert or replace into projects (id,project) values ((select ID from projects where project = :project), :project); '''
INSERT_JOBS=''' insert or replace into jobs (id,jobid,user,project,ncpus) values ((select ID from jobs where jobid = :jobid), :jobid, :user ,:project, :ncpus); '''
INSERT_NODE=''' insert or replace into node (id,node) values ((select ID from node where node = :node), :node); '''
INSERT_SOFTWARE=''' insert or replace into software (id,path) values ((select ID from software where path = :software), :software); '''
INSERT_COMMAND='''
insert or replace into command (id,jobid,node,software,start_time,end_time,user,sys,updated) values
(
        (
                select ID from command 
                        where 
                                jobid = :id
                        and 
                                node  = :node_id
                        and
                                software = :sw_id
        )
, 
        :id,
        :node_id,
        :sw_id,
        :start_time,:end_time,
        :user,:sys,
        strftime('%s','now')
);
'''

UPDATE_MINMAX='''
update jobs set 
    start_time = :start_time,
    end_time = :end_time,
    user_time = :user_time,
    system_time = :system_time
where id = :id
'''

FIND_MINMAX_JOBS='''
select jobid,min(start_time),max(end_time),sum(user),sum(sys) 
from command 
where jobid in (select id from jobs where start_time is null or end_time is null or user_time is null or system_time is null)
group by jobid
'''

class Aggregator(sams.base.Aggregator):
    """ SAMS Software accounting aggregator """
    def __init__(self,id,config):
        super(Aggregator,self).__init__(id,config)
        self.db = {}
        self.db_path = self.config.get([self.id,'db_path'])
        self.file_pattern = self.config.get([self.id,'file_pattern'],"sa-%(jobid_hash)d.db")
        self.jobid_hash_size = self.config.get([self.id,'jobid_hash_size'],1000000000)
        self.inserted = {}

    def _open_db(self,jobid_hash):
        """ Open database object """
        db = os.path.join(self.db_path,self.file_pattern % { 'jobid_hash': int(jobid_hash) })        
        self.db[jobid_hash] = sqlite3.connect(db)
        self.db[jobid_hash].isolation_level = None
        c = self.db[jobid_hash].cursor()
        for sql in TABLES:
            logger.debug(sql)
            c.execute(sql)
        self.db[jobid_hash].commit()
        return self.db[jobid_hash]

    def get_db(self,jobid):
        """ get db connection based on jobid / jobid_hash_size """
        jobid_hash = int(jobid / self.jobid_hash_size)
        if jobid_hash in self.db:
            return self.db[jobid_hash]
        return self._open_db(jobid_hash)

    def save_id(self,jobid,table,value,id):
        """ Only try to insert once / session """
        jobid_hash = int(jobid / self.jobid_hash_size)
        if value not in self.inserted[jobid_hash][table]:
            self.inserted[jobid_hash][table][value] = id
        logger.debug("save_id(%d (%d),%s,%s,%d)" % (jobid,jobid_hash,table,value,id))
        return id

    def get_id(self,jobid,table,value):
        """ Only try to insert once / session """
        jobid_hash = int(jobid / self.jobid_hash_size)
        logger.debug("get_id(%d (%d),%s,%s,%d)" % (jobid,jobid_hash,table,value,self.inserted[jobid_hash][table][value]))
        return self.inserted[jobid_hash][table][value]

    def do_insert(self,jobid,table,value):
        """ Only try to insert once / session """
        jobid_hash = int(jobid / self.jobid_hash_size)
        if jobid_hash not in self.inserted:
            self.inserted[jobid_hash] = {}
        if table not in self.inserted[jobid_hash]:
            self.inserted[jobid_hash][table] = {}
        if value not in self.inserted[jobid_hash][table]:
            return True
        return False

    def aggregate(self,data):
        """ Information aggregate method """

        jobid = int(data['sams.sampler.Core']['jobid'])
        node = data['sams.sampler.Core']['node']

        # Get database for jobid
        db = self.get_db(jobid)
        c = db.cursor()

        for module in [ 'sams.sampler.Software', 'sams.sampler.SlurmInfo' ]:
            if not module in data:
                logger.info("Jobid: %d on node %s has no %s" % (jobid,node,module))
                raise Exception("Jobid: %d on node %s has no %s" % (jobid,node,module))

        # Begin transaction
        c.execute('BEGIN TRANSACTION')

        # If project (account) is defined in data insert into table
        project = None        
        project_id = None
        if 'account' in data['sams.sampler.SlurmInfo']:
            project = data['sams.sampler.SlurmInfo']['account']
            if self.do_insert(jobid,'projects',project):
                c.execute(INSERT_PROJECT,{ 'project' : project })
                project_id = self.save_id(jobid,'projects',project,c.lastrowid)
                logger.debug("Inserted project: %s as %d (%d)" % (project,c.lastrowid,project_id))
            else:
                project_id = self.get_id(jobid,'projects',project)
                logger.debug("Fetched project: %s as %d" % (project,project_id))


        # If username is defined in data insert into table
        user = None
        user_id = None
        if 'username' in data['sams.sampler.SlurmInfo']:
            user = data['sams.sampler.SlurmInfo']['username']
            if self.do_insert(jobid,'users',user):
                c.execute(INSERT_USER,{ 'user': user })
                user_id = self.save_id(jobid,'users',user,c.lastrowid)
                logger.debug("Inserted user: %s as %d (%d)" % (user,c.lastrowid,user_id))
            else:
                user_id = self.get_id(jobid,'users',user)
                logger.debug("Fetched user: %s as %d" % (user,user_id))

        # If username is defined in data insert into table
        ncpus = None
        if 'cpus' in data['sams.sampler.SlurmInfo']:
            ncpus = data['sams.sampler.SlurmInfo']['cpus']
    
        # Insert information about job
        c.execute(INSERT_JOBS,{'jobid': jobid, 'user':user,'project':project,'ncpus':ncpus})
        id = c.lastrowid

        # Insert node
        node_id = None
        if self.do_insert(jobid,'nodes',node):        
            c.execute(INSERT_NODE,{ 'node': node })
            node_id = self.save_id(jobid,'nodes',node,c.lastrowid)
            logger.debug("Inserted node: %s as %d (%d)" % (node,c.lastrowid,node_id))
        else:
            node_id = self.get_id(jobid,'nodes',node)

        # Insert information about running commands
        for sw,info in data['sams.sampler.Software']['execs'].items():
            # Insert software
            sw_id = None
            if self.do_insert(jobid,'softwares',sw):
                c.execute(INSERT_SOFTWARE,{ 'software': sw })
                sw_id = self.save_id(jobid,'softwares',sw,c.lastrowid)
                logger.debug("Inserted sw: %s as %d (%d)" % (sw,c.lastrowid,sw_id))
            else:
                sw_id = self.get_id(jobid,'softwares',sw)
                logger.debug("Fetched sw: %s as %d" % (sw,sw_id))

            c.execute(INSERT_COMMAND,{
                'id': id,
                'node_id': node_id,
                'sw_id': sw_id,
                'start_time': int(data['sams.sampler.Software']['start_time']),
                'end_time': int(data['sams.sampler.Software']['end_time']),
                'user': info['user'],
                'sys': info['system']
            })

        # Commit data to disk
        c.execute('COMMIT')
        db.commit()

    def cleanup(self):
        for id,db in self.db.items():
            db.rollback()

    def close(self):
        for id,db in self.db.items():
            # Update jobs table.
            try:
                c = db.cursor()
                c.execute('BEGIN TRANSACTION')
                rows = [row for row in c.execute(FIND_MINMAX_JOBS)]
                for row in rows:
                    c.execute(UPDATE_MINMAX,{
                        'id': row[0],
                        'start_time': row[1],
                        'end_time': row[2],
                        'user_time': row[3],
                        'system_time': row[4]
                    })
                c.execute('COMMIT')
                db.commit()
            except Exception as e:
                logger.exception(e)

            db.close()
