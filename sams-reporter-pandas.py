

import argparse

import sqlite3
import pandas as pd

from time import mktime
from datetime import datetime

class CommaSeparatedStringsAction(argparse.Action):
    def __init__(self, option_strings, dest, nargs=None, **kwargs):
        if nargs is not None:
            raise ValueError("nargs not allowed")
        super(CommaSeparatedStringsAction, self).__init__(option_strings, dest, **kwargs)
    def __call__(self, parser, namespace, values, option_string=None):
        print('%r %r %r' % (namespace, values, option_string))
        values = values.split(',')
        setattr(namespace, self.dest, values)

strings_opt = dict(type=str, default=[], action=CommaSeparatedStringsAction)

parser = argparse.ArgumentParser(description='Generate report on software usage')
parser.add_argument('-s', '--software', help='Only consider jobs using the given software(s)', **strings_opt)
parser.add_argument('-Is', '--ignore-software', help='Ignore jobs using the given software(s)', **strings_opt)
parser.add_argument('-u', '--user', help='Only consider jobs for the given user(s)', **strings_opt)
parser.add_argument('-Iu', '--ignore-user', help='Ignore jobs for the given user(s)', **strings_opt)
parser.add_argument('-a', '--project', help='Only consider jobs for the given project(s)', **strings_opt)
parser.add_argument('-Ia', '--ignore-project', help='Ignore jobs for the given project(s)', **strings_opt)
args = parser.parse_args()

print(args)
#exit()

filename = 'sa-0.db'
print('--> sqlite3 connect')
conn = sqlite3.connect('file:{}?mode=ro'.format(filename), uri=True)

start = int(datetime.strptime('2019-11-01', '%Y-%m-%d').timestamp())
stop = int(datetime.strptime('2019-11-05', '%Y-%m-%d').timestamp())

print(datetime.fromtimestamp(start), datetime.fromtimestamp(stop))

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
jobs = pd.read_sql_query("select * from jobs where end_time >= {} AND start_time <= {};".format(start, stop), conn)
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
## -- Check that jobid:s are everywhere
#for job_id in jobs.id:
#    print(job_id)
#    print(( commands.job_id == job_id ).sum())
#    assert( ( commands.job_id == job_id ).sum() == 1 )
#exit()

# for each software
# usage per user and project
# total usage

#print(softwares)
#print(softwares.columns)

#print(commands)
#print(commands.columns)
#print(commands.software)

for software in softwares.itertuples():
    #print(row)
    job_ids = commands[commands.software_id == software.id].job_id
    if len(job_ids) == 0:
        continue
    #print('job_ids:\n', list(job_ids))
    #print('jobs.id:\n', list(jobs.id))
    jobs = jobs[ jobs.id.isin(job_ids) ]

    if len(jobs) > 0:
        print('jobs:\n', jobs)
    #print(len(jobs))
    
