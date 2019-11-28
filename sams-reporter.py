#!/usr/bin/env python3

"""
Software reporter for SAMS Software accounting
"""

from __future__ import print_function
from collections import defaultdict
from time import mktime
from datetime import datetime

import argparse
import sqlite3
import sys

# We will have 'my_id' as key for all tables, i.e. it is mandatory
sws_columns = ['my_id', 'path', 'software', 'version', 'versionstr', 'user_provided']
jobs_columns = ['my_id', 'slurm_id', 'user', 'project', 'ncpus', 'start_time', 'end_time', 'user_time', 'system_time']
cmds_columns = ['my_id', 'job_id', 'node', 'sw_id', 'start_time', 'end_time', 'user', 'sys', 'updated']

parser = argparse.ArgumentParser(description='Generate report on software usage')
pg0 = parser.add_argument_group('Time options')
pg00 = pg0.add_mutually_exclusive_group()
pg00.add_argument('-b', '--begin', type=str, help='First day of time interval to consider (format YYYY-MM-DD)')
pg00.add_argument('-d', '--days', type=int, help='Look this many days (24h periods) back')
pg0.add_argument('-e', '--end', type=str, help='First day _after_ time interval to consider, default "now"')
pg1 = parser.add_argument_group('Selection options')
pg11 = pg1.add_mutually_exclusive_group()
pg11.add_argument('-u', '--user', type=str, default='', help='Only consider jobs for the given user(s)')
pg11.add_argument('-Iu', '--ignore-user', type=str, default='', help='Ignore jobs for the given user(s)')
pg12 = pg1.add_mutually_exclusive_group()
pg12.add_argument('-a', '--account', type=str, default='', help='Only consider jobs for the given account/project(s)')
pg12.add_argument('-Ia', '--ignore-account', type=str, default='', help='Ignore jobs for the given account/project(s)')
pg13 = pg1.add_mutually_exclusive_group()
pg13.add_argument('-s', '--software', type=str, default='', help='Only consider jobs using the given software(s)')
pg13.add_argument('-Is', '--ignore-software', type=str, default='', help='Ignore jobs using the given software(s)')
pg2 = parser.add_argument_group('Details options')
pg2.add_argument('-lu', '--list-users', action='store_true', help='List individual users')
pg2.add_argument('-la', '--list-accounts', action='store_true', help='List individual accounts/projects')
pg2.add_argument('-lv', '--list-versions', action='store_true', help='List software versions')
pg3 = parser.add_argument_group('General aggregation options (i.e. do not list versions, accounts or users for this' +
                                ' software)')
pg3.add_argument('-U', '--aggregate-sw-user', action='store_true', help='Aggregate user usage')
pg3.add_argument('-G', '--aggregate-sw-group', action='store_true', help='Aggregate group usage')
pg3.add_argument('-S', '--aggregate-sw-system', action='store_true', help='Aggregate system usage')
pg4 = parser.add_argument_group('General options')
pg4.add_argument('-f', '--file', type=str, help='DB-file to get data from', default='sa-0.db')
pg4.add_argument('-l', '--lower', type=float, default=0.01,
                 help="Don't list software with lower core-h usage than this (default 0.01)")
pg4.add_argument('-j', '--jobs', action='store_true', help='Sort by number of jobs instead of usage')
pg4.add_argument('-v', '--verbose', action='store_true', help='Provide some more information')
pg5 = parser.add_argument_group('Dump commands')
pg5.add_argument('-dc', '--dump-commands', action='store_true', help='Dump out list of commands and exit')


def load_db_table(table_name: str, condition: str = '') -> list:
    if condition:
        return list(db_cursor.execute('SELECT * FROM {} WHERE {}'.format(table_name, condition)))
    else:
        return list(db_cursor.execute('SELECT * FROM {}'.format(table_name)))


def load_table(table_name: str, headers: list, condition: str = '') -> dict:
    items = load_db_table(table_name, condition)

    full_dict = {}
    for item in items:
        my_dict = dict(zip(headers, item))
        my_id = my_dict['my_id']
        my_dict.pop('my_id')
        # print(my_id, my_dict)
        full_dict[my_id] = my_dict

    return full_dict


#@profile
# TODO: Re-do the logic so that we only loop through cmds once, i.e. get rid of loop in main that calls this routine1
def get_sw_usage(sw_ids, list_items: bool = False) -> defaultdict:
    usages = defaultdict(float)
    usages['jobs'] = set()

    if args.list_versions:
        usages['versions'] = defaultdict(lambda: defaultdict(float))

    cmds = [cmd for cmd in db_cmds.values() if cmd['sw_id'] in sw_ids]

    for cmd in cmds:
#    for cmd in db_cmds.values():
        # print('  ', sw, sw_ids, cmd['sw_id'])
        # print(cmd)
#        if not cmd['sw_id'] in sw_ids:
#            continue

        if not cmd['job_id'] in db_jobs:
            continue

        job = db_jobs[cmd['job_id']]
        if args.user and job['user'].lower() not in args.user:
            continue
        if args.ignore_user and job['user'].lower() in args.ignore_user:
            continue

        if args.account and job['project'] not in args.account:
            continue
        if args.ignore_account and job['project'] in args.ignore_account:
            continue

        job_start = job['start_time']
        if job_start < start:
            job_start = min(start, job['end_time'])
        job_stop = job['end_time']
        if job_stop > stop:
            job_stop = max(stop, job['start_time'])

        # Only consider the part of the usage in the current time window
        if job_stop - job_start:
            f = (job_stop - job_start) / (job['end_time'] - job['start_time'])
        else:  # Zero length job!?
            f = 1.0

        cmd_usage = f * (cmd['user'] + cmd['sys']) / 3600
        job_wtime = (stop - start) / 3600

        usages['total'] += cmd_usage
        usages['wsum'] += job_wtime * job['ncpus']
        usages['time'] += job_wtime
        usages['jobs'].add(cmd['job_id'])

        if not list_items:
            continue

        if args.list_versions:
            version = db_sws[cmd['sw_id']]['version']
            next_lvl = usages['versions'][version]
            next_lvl['usage'] += cmd_usage
            next_lvl['wsum'] += job_wtime * job['ncpus']
            next_lvl['time'] += job_wtime
        else:
            next_lvl = usages
        if args.list_accounts:
            account = job['project']
            if 'accounts' not in next_lvl:
                next_lvl['accounts'] = defaultdict(lambda: defaultdict(float))
            next_lvl = next_lvl['accounts'][account]
            next_lvl['usage'] += cmd_usage
            next_lvl['wsum'] += job_wtime * job['ncpus']
            next_lvl['time'] += job_wtime
        if args.list_users:
            user = job['user']
            if 'users' not in next_lvl:
                next_lvl['users'] = defaultdict(lambda: defaultdict(float))
            next_lvl['users'][user]['usage'] += cmd_usage
            next_lvl['users'][user]['wsum'] += job_wtime * job['ncpus']
            next_lvl['users'][user]['time'] += job_wtime

    return usages


def print_usage_lines(items: defaultdict, total: float, levels: int, indent: int = 1) -> None:
    for name, val in sorted(items.items(), key=lambda v: v[1]['usage'], reverse=True):
        if not name:
            name = '-'
        if total < 1e-3:
            return
        print((' ' * 3 * indent + '{:25}  {:5.1f}%' + ' ' * (42 + 3 * levels) + '{:4}').format(
            name, 100 * val['usage'] / total, round(val['wsum'] / val['time'])))
        if 'accounts' in val:
            print_usage_lines(val['accounts'], val['usage'], levels, indent + 1)
        elif 'users' in val:
            print_usage_lines(val['users'], val['usage'], levels, indent + 1)


def dump_commands() -> None:
    for sw in load_table('software', sws_columns).values():
        print(sw['path'])


args = parser.parse_args()

# Connect to our DB
try:
    db_con = sqlite3.connect('file:{}?mode=ro'.format(args.file), uri=True)
except sqlite3.OperationalError as e:
    print('Unable to access database file "{}", exiting'.format(args.file))
    sys.exit(1)

db_cursor = db_con.cursor()

if args.dump_commands:
    dump_commands()
    sys.exit(0)

# These "software" will not reported on a version level
sw_to_aggregate = []
if args.aggregate_sw_user:
    sw_to_aggregate.append('user')
if args.aggregate_sw_group:
    sw_to_aggregate.append('group')
if args.aggregate_sw_system:
    sw_to_aggregate.append('system')

if args.end:
    try:
        stop = int(datetime.strptime(args.end, '%Y-%m-%d').timestamp())
    except ValueError as e:
        print('Invalid format for --end: "{}"; {}'.format(args.end, e))
        sys.exit(1)
else:
    stop = int(mktime(datetime.now().timetuple()))

if args.begin:
    try:
        start = int(datetime.strptime(args.begin, '%Y-%m-%d').timestamp())
    except ValueError as e:
        print('Invalid format for --begin: "{}"; {}'.format(args.begin, e))
        sys.exit(1)
elif args.days:
    start = stop - args.days * 24 * 60 * 60
else:
    start = int(datetime.strptime('2018-10-23', '%Y-%m-%d').timestamp())

if stop < start:
    print('Start date after end date, exiting!')
    sys.exit(1)

if args.user:
    print('Showing usage for user(s) "{}"'.format(args.user))
    args.user = [x.lower() for x in args.user.split(',')]
if args.account:
    print('Showing usage for account/project(s) "{}"'.format(args.account))
    args.account = [x.lower() for x in args.account.split(',')]
if args.software:
    print('Showing usage for software(s) "{}"'.format(args.software))
    args.software = [x.lower() for x in args.software.split(',')]

if args.ignore_user:
    print('Ignoring usage for user(s) "{}"'.format(args.ignore_user))
    args.ignore_user = [x.lower() for x in args.ignore_user.split(',')]
if args.ignore_account:
    print('Ignoring usage for account/project(s) "{}"'.format(args.ignore_account))
    args.ignore_account = [x.lower() for x in args.ignore_account.split(',')]
if args.ignore_software:
    print('Ignoring usage for software(s) "{}"'.format(args.ignore_software))
    args.ignore_software = [x.lower() for x in args.ignore_software.split(',')]

print('Including jobs in the interval {} - {}'.format(datetime.fromtimestamp(start),
                                                      datetime.fromtimestamp(stop)))
print()

# Load tables
db_sws = load_table('software', sws_columns)
db_jobs = load_table('jobs', jobs_columns, 'end_time >= {} AND start_time <= {}'.format(start, stop))
db_cmds = load_table('command', cmds_columns)

if args.verbose:
    print('Loaded {} jobs, {} softwares and {} commands\n'.format(len(db_jobs), len(db_sws), len(db_cmds)))

softwares = defaultdict(set)
for my_id, sw in db_sws.items():
    name = sw['software']
    if args.verbose and not name:
        print('Warning, no software for path {}!\n'.format(sw['path']))
    if args.software and (not name or name.lower() not in args.software):
        continue
    if args.ignore_software and (name and name.lower() in args.ignore_software):
        continue
    softwares[name].add(my_id)

usage = dict()
grand_total = 0
for name, ids in softwares.items():
    list_items = name not in sw_to_aggregate
    usages = get_sw_usage(ids, list_items)
    grand_total += usages['total']
    usage[name] = dict(usages)

if grand_total < args.lower:
    print('No jobs matching the given criteria found, exiting')
    sys.exit(0)

# Sum no of jobs
grand_total_jobs = 0
for us in usage.values():
    us['no_jobs'] = len(us['jobs'])
    grand_total_jobs += us['no_jobs']

# Print usage table
ver_indent = 5 if args.list_versions else 0
acc_indent = 5 if args.list_accounts else 0
usr_indent = 5 if args.list_users else 0
all_indent = ver_indent + acc_indent + usr_indent
levels = int(all_indent / 5)
print('Software' + ' ' * (33 + all_indent) + 'core-h percentage  jobs percentage  cores')
if args.list_versions:
    print(' ' * ver_indent + 'version' + ' ' * 15 + 'percentage')
if args.list_accounts:
    print(' ' * (ver_indent + acc_indent) + 'account' + ' ' * 15 + 'percentage')
if args.list_users:
    print(' ' * all_indent + 'user   ' + ' ' * 15 + 'percentage')

print('-' * (83 + all_indent * 2))
for name, sw in sorted(usage.items(), key=lambda v: v[1]['no_jobs' if args.jobs else 'total'], reverse=True):
    # print(name, sw)
    total = sw.pop('total')
    jobs = sw.pop('no_jobs')
    if total < args.lower or not jobs:
        continue

    if not name:
        name = '-'
    print(('{:25}' + ' ' * (14 + all_indent) + '{:8}    {:5.1f}% {:7} {:5.1f}%     {:4}').
          format(name, int(total), 100 * total / grand_total, jobs, 100 * jobs / grand_total_jobs, round(sw['wsum'] / sw['time'])))

    if 'versions' in sw:
        print_usage_lines(sw['versions'], total, levels)
    elif 'accounts' in sw:
        print_usage_lines(sw['accounts'], total, levels)
    elif 'users' in sw:
        print_usage_lines(sw['users'], total, levels)
