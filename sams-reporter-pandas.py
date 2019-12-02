"""
Read SAMS Software Accounting sqllite3 database and produce accumulated report.

Authors: Thomas Svedberg (2019)
         Hugo U.R. Strand (2019) 

"""

import argparse

import sqlite3
import pandas as pd

import dateutil
from time import mktime
from datetime import datetime, timedelta

class CommaSeparatedStringsAction(argparse.Action):
    def __init__(self, option_strings, dest, nargs=None, **kwargs):
        if nargs is not None:
            raise ValueError("nargs not allowed")
        super(CommaSeparatedStringsAction, self).__init__(option_strings, dest, **kwargs)
    def __call__(self, parser, namespace, values, option_string=None):
        #print('%r %r %r' % (namespace, values, option_string))
        values = values.split(',')
        setattr(namespace, self.dest, values)

def get_args():
    strings_opt = dict(type=str, default=[], action=CommaSeparatedStringsAction)

    parser = argparse.ArgumentParser(description='Generate report on software usage')

    p_gen = parser.add_argument_group('General options')
    p_gen.add_argument(
        '-f', '--filename', help='DB-file to get data from',
        type=str, default='sa-0.db')
    p_gen.add_argument('-v', '--verbose', action='store_true', help='Verbose output')

    p_time = parser.add_argument_group('Time options')
    p_time.add_argument(
        '-b', '--begin', help='Beginning of time interval',
        type=dateutil.parser.parse, default=dateutil.parser.parse('2010-01-10'))
    p_time.add_argument(
        '-e', '--end', help='End of time interval',
        type=dateutil.parser.parse, default=datetime.now())

    p_select = parser.add_argument_group('Selection options')
    p_select.add_argument(
        '-s', '--software', help='Only consider jobs using the given software(s)',
        **strings_opt)
    p_select.add_argument(
        '-Is', '--ignore-software', help='Ignore jobs using the given software(s)',
        **strings_opt)
    p_select.add_argument(
        '-u', '--user', help='Only consider jobs for the given user(s)',
        **strings_opt)
    p_select.add_argument(
        '-Iu', '--ignore-user', help='Ignore jobs for the given user(s)',
        **strings_opt)
    p_select.add_argument(
        '-a', '--project', help='Only consider jobs for the given project(s)',
        **strings_opt)
    p_select.add_argument(
        '-Ia', '--ignore-project', help='Ignore jobs for the given project(s)',
        **strings_opt)

    args = parser.parse_args()
    return args

class SAMSSoftwareAccountingDB:
    def __init__(self, args):

        self.args = args
        self.verbose = args.verbose

        self._open_db()
        self._load_tables()
        #self._print_column_labels()
        self._filter_tables()
        self._compute_times()
        self._print_column_labels()
        self._combine_tables()

    def _open_db(self):
        if self.verbose: print('Loading sqlite3 database file: {}'.format(args.filename))
        self.conn = sqlite3.connect('file:{}?mode=ro'.format(args.filename), uri=True)

    def _load_tables(self):
        
        start, stop = int(args.begin.timestamp()), int(args.end.timestamp())

        query = "select * from "
        time_query = " where end_time >= {} AND start_time <= {};".format(start, stop)
        
        if self.verbose: print('--> Querying "software" table')
        softwares = pd.read_sql_query(query + "software", self.conn)

        if self.verbose: print('--> Querying "command" table')
        commands = pd.read_sql_query(query + "command" + time_query, self.conn)

        if self.verbose: print('--> Querying "jobs" table')
        jobs = pd.read_sql_query(query + "jobs" + time_query, self.conn)
        
        # -- Rename columns in data frames
        softwares.rename(columns={'software':'name'}, inplace=True)
        
        commands.rename(columns={'software':'software_id'}, inplace=True)
        commands.rename(columns={'jobid':'job_id'}, inplace=True)

        jobs.rename(columns={'jobid':'slurm_id'}, inplace=True)
        jobs.rename(columns={'start_time':'job_start_time'}, inplace=True)
        jobs.rename(columns={'end_time':'job_end_time'}, inplace=True)
        jobs.rename(columns={'user':'job_user'}, inplace=True)

        self.softwares, self.commands, self.jobs = softwares, commands, jobs

    def _print_column_labels(self):
        
        if self.verbose:
            softwares, commands, jobs = self.softwares, self.commands, self.jobs
            print('commands labels:', list(commands.columns))
            print('software labels:', list(softwares.columns))
            print('jobs labels:', list(jobs.columns))

    def _filter_tables(self):

        softwares, commands, jobs = self.softwares, self.commands, self.jobs

        def select_include_exclude(df, key, include, exclude):
            select_idx = df[key].isin(include) & ~df[key].isin(exclude)
            return df[select_idx]

        if len(args.user) > 0 or len(args.ignore_user) > 0:
            commands = select_include_exclude(commands, 'user', args.user, args.ignore_user)
            jobs = select_include_exclude(jobs, 'user', args.user, args.ignore_user)

        if len(args.project) > 0 or len(args.ignore_project) > 0:
            commands = select_include_exclude(commands, 'project', args.project, args.ignore_project)
            jobs = select_include_exclude(jobs, 'project', args.project, args.ignore_project)

        if len(args.software) > 0 or len(args.ignore_software) > 0:
            softwares = select_include_exclude(softwares, 'name', args.software, args.ignore_software)
            commands = select_include_exclude(commands, 'software_id', softwares.id, [])
            jobs = jobs[ jobs.id.isin(commands.job_id) ]

        self.softwares, self.commands, self.jobs = softwares, commands, jobs
            
    def _compute_times(self):
        
        softwares, commands, jobs = self.softwares, self.commands, self.jobs

        to_datetime = [
            (jobs, ['job_start_time', 'job_end_time']),
            (commands, ['start_time', 'end_time']),
            ]

        for df, keys in to_datetime:
            for key in keys:
                df[key] = pd.to_datetime(df[key], unit='s')
        
        jobs.user_time = pd.to_timedelta(jobs.user_time, unit='s')
        jobs.system_time = pd.to_timedelta(jobs.system_time, unit='s')

        jobs['total_time'] = jobs.user_time + jobs.system_time
        jobs.total_time /= timedelta(hours=1)
        
        jobs['cpu_time'] = jobs.total_time * jobs.ncpus 
        
        self.softwares, self.commands, self.jobs = softwares, commands, jobs

    def _combine_tables(self):

        data = self.commands.copy()
        data = data.join(self.jobs.set_index('id'), on='job_id')
        data = data.join(self.softwares.set_index('id'), on='software_id')
        self.data = data
        
if __name__ == '__main__':

    args = get_args()
    sdb = SAMSSoftwareAccountingDB(args)

    data = sdb.data.copy()

    total = data.groupby(['name', 'version', 'project', 'job_user'])
    total = total.agg({'total_time' : 'sum', 'cpu_time' : 'sum'})
    print(total)

    project_total = data.groupby(['project', 'name'])
    project_total = project_total.agg({'total_time' : 'sum', 'cpu_time' : 'sum'})
    project_total = project_total.sort_values(by='cpu_time', ascending=False)
    print(project_total)

    user_total = data.groupby(['job_user', 'name'])
    user_total = user_total.agg({'total_time' : 'sum', 'cpu_time' : 'sum'})
    user_total = user_total.sort_values(by='cpu_time', ascending=False)
    print(user_total)
    
