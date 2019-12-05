"""
Read SAMS Software Accounting sqllite3 database and produce accumulated report.

Authors: Hugo U.R. Strand (2019) 
         Thomas Svedberg (2019)

"""

import argparse

import sqlite3
import numpy as np
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

    p_sort= parser.add_argument_group('Sort options').add_mutually_exclusive_group()
    p_sort.add_argument('-st', '--sort_time', action='store_true', help='Sort on time (core-h) usage (default)')
    p_sort.add_argument('-sj', '--sort_jobs', action='store_true', help='Sort on number of jobs')
    p_sort.add_argument('-sc', '--sort_cpus', action='store_true', help='Sort on number of cpus/h')
    
    p_list = parser.add_argument_group('List selection')
    p_list.add_argument('-lv', '--list_version', action='store_true', help='Show software versions')
    p_list.add_argument('-lp', '--list_project', action='store_true', help='Show project names')
    p_list.add_argument('-lu', '--list_user', action='store_true', help='Show user names')

    p_time = parser.add_argument_group('Time options')
    p_time.add_argument('-b', '--begin', help='Beginning of time interval',
        type=dateutil.parser.parse, default=dateutil.parser.parse('2010-01-10'))
    p_time.add_argument('-e', '--end', help='End of time interval',
        type=dateutil.parser.parse, default=datetime.now())

    p_select = parser.add_argument_group('Selection options')
    p_select.add_argument('-s', '--software', help='Only consider jobs using the given software(s)', **strings_opt)
    p_select.add_argument('-Is', '--ignore-software', help='Ignore jobs using the given software(s)', **strings_opt)
    p_select.add_argument('-u', '--user', help='Only consider jobs for the given user(s)', **strings_opt)
    p_select.add_argument('-Iu', '--ignore-user', help='Ignore jobs for the given user(s)', **strings_opt)
    p_select.add_argument('-p', '--project', help='Only consider jobs for the given project(s)',  **strings_opt)
    p_select.add_argument('-Ip', '--ignore-project', help='Ignore jobs for the given project(s)', **strings_opt)

    args = parser.parse_args()
    return args

class SAMSSoftwareAccountingDB:
    def __init__(self, args):

        self.args = args
        self.verbose = args.verbose

        self._print_header()
        self._open_db()
        self._load_tables()
        self._filter_tables()
        self._compute_times()
        #self._print_column_labels()
        self._combine_tables()

    def _print_header(self):
        if self.verbose: print('Loading SAMS-Software Accounting data from', self.args.begin, 'to', self.args.end)
        
    def _open_db(self):
        if self.verbose: print('Using sqlite3 database file: {}'.format(args.filename))
        self.conn = sqlite3.connect('file:{}?mode=ro'.format(args.filename), uri=True)

    def _load_tables(self):
        
        start, stop = int(args.begin.timestamp()), int(args.end.timestamp())

        # -- Query database for three tables
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
            #commands = select_include_exclude(commands, 'user', args.user, args.ignore_user)
            jobs = select_include_exclude(jobs, 'job_user', args.user, args.ignore_user)

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
        jobs['no_jobs'] = 1
        
        self.softwares, self.commands, self.jobs = softwares, commands, jobs

    def _combine_tables(self):

        data = self.commands.copy()
        data = data.join(self.jobs.set_index('id'), on='job_id')
        data = data.join(self.softwares.set_index('id'), on='software_id')
        self.data = data

    def get_pandas_data_frame(self):
        return self.data.copy()
        
if __name__ == '__main__':

    args = get_args()
    sdb = SAMSSoftwareAccountingDB(args)

    column_labels = { 'name' : 'Software' }
    if args.list_version: column_labels['version'] = 'Version'
    if args.list_project: column_labels['project'] = 'Project'
    if args.list_user: column_labels['job_user'] = 'User'

    agg_opts = {'total_time' : 'sum', 'cpu_time' : 'sum', 'no_jobs' : 'sum'}    

    sdf = sdb.get_pandas_data_frame()
    sdf = sdf.groupby(list(column_labels.keys()))
    sdf = sdf.agg(agg_opts)
    index_names = list(column_labels.values())
    sdf.index.set_names(index_names, inplace=True)

    def recurse_indices(df, column_key, prev_index={}):
        """ Recurse indices to compute relative percentage of `column_key` 
        for each sub-group of indices. Returns a list of (key, value) pair dicts. """

        # -- sum over all under key
        index_name = df.index.names[0]
        df_agg = df.groupby([index_name])
        df_agg = df_agg.agg(agg_opts)

        # -- Compute cpus averaged per hour (instead of per job)
        df_agg['no_cpus'] = df_agg.cpu_time / df_agg.total_time
        df_agg.pop('total_time')
        df_agg['percent'] = 100. * df_agg[column_key] / df_agg[column_key].sum()

        # -- sort on column_key
        df_agg = df_agg.sort_values(by=column_key, ascending=False)
        
        rows = []        
        for key in df_agg.index:
            # -- Add percentage in key (for display purposes)
            index = prev_index
            index[index_name] = '{:5.1f}% '.format(df_agg.loc[key]['percent']) + key

            if len(df.index.names) == 1:
                row = index
                row.update(df_agg.loc[key])
                rows.append(row.copy())
            else:
                rows += recurse_indices(df.loc[key], column_key, prev_index=index)

        return rows

    if args.sort_time: column_key = 'cpu_time'
    elif args.sort_jobs: column_key = 'no_jobs'
    elif args.sort_cpus: column_key = 'no_cpus'
    else: column_key = 'cpu_time'
        
    # -- Recurse and put result in a pandas data frame for printing
    rows = recurse_indices(df=sdf, column_key=column_key)
    
    column_labels = ['cpu_time', 'no_jobs', 'no_cpus']
    agg_opts = { label : 'sum' for label in column_labels }
    summary = pd.DataFrame(rows, columns = index_names + column_labels)
    summary = summary.groupby(index_names, sort=False).agg(agg_opts)

    # -- Set up column data formatters
    formatters = dict()
    formatters.update({ 'cpu_time' : lambda x : '{:10.0f}'.format(x) })
    formatters.update({ 'no_jobs' : lambda x : '{:3.0f}'.format(x) })
    formatters.update({ 'no_cpus' : lambda x : '{:3.1f}'.format(x) })

    # -- Print table
    if column_key is 'cpu_time': print('Percentage of time (core-hours).')
    if column_key is 'no_jobs': print('Percentage of total number of jobs.')
    if column_key is 'no_cpus': print('Percentage of average number of cpus per hour.')
    print(summary.to_string(formatters=formatters, header=['Time (Core-h)', 'Jobs', 'Cpus/h']))
