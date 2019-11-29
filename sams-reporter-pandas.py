

import argparse

import sqlite3
import pandas as pd

from time import mktime
from datetime import datetime
import dateutil

class CommaSeparatedStringsAction(argparse.Action):
    def __init__(self, option_strings, dest, nargs=None, **kwargs):
        if nargs is not None:
            raise ValueError("nargs not allowed")
        super(CommaSeparatedStringsAction, self).__init__(option_strings, dest, **kwargs)
    def __call__(self, parser, namespace, values, option_string=None):
        #print('%r %r %r' % (namespace, values, option_string))
        values = values.split(',')
        setattr(namespace, self.dest, values)

strings_opt = dict(type=str, default=[], action=CommaSeparatedStringsAction)

parser = argparse.ArgumentParser(description='Generate report on software usage')

p_gen = parser.add_argument_group('General options')
p_gen.add_argument(
    '-f', '--filename', help='DB-file to get data from',
    type=str, default='sa-0.db')

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

print('--> sqlite3 connect')
conn = sqlite3.connect('file:{}?mode=ro'.format(args.filename), uri=True)

start = int(args.begin.timestamp())
stop = int(args.end.timestamp())

print('--> software')
softwares = pd.read_sql_query("select * from software;", conn)
softwares.rename(columns={'software':'name'}, inplace=True)
print(softwares.columns)

print('--> commands')
commands = pd.read_sql_query(
    "select * from command where end_time >= {} AND start_time <= {};".format(start, stop), conn)
commands.rename(columns={'software':'software_id'}, inplace=True)
commands.rename(columns={'jobid':'job_id'}, inplace=True)
print(commands.columns)

print('--> jobs')
jobs = pd.read_sql_query(
    "select * from jobs where end_time >= {} AND start_time <= {};".format(start, stop), conn)
jobs.rename(columns={'jobid':'slurm_id'}, inplace=True)

print(jobs.columns)

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
    
    #print(software['software'])
    #for row in software.iterrows():
    #    print(row)

for df in [jobs, commands]:
    df.start_time = pd.to_datetime(df.start_time, unit='s')
    df.end_time = pd.to_datetime(df.end_time, unit='s')

jobs.user_time = pd.to_timedelta(jobs.user_time, unit='s')
jobs.system_time = pd.to_timedelta(jobs.system_time, unit='s')
jobs.total_time = jobs.user_time + jobs.system_time
jobs.cpu_time = jobs.total_time * jobs.ncpus

for software in softwares.itertuples():
    job_ids = commands[commands.software_id == software.id].job_id
    if len(job_ids) == 0:
        continue
    jobs = jobs[ jobs.id.isin(job_ids) ]

    if len(jobs) > 0:
        print('jobs:\n', jobs)
    #print(len(jobs))
    
