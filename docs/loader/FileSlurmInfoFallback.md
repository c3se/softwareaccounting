# sams.loader.FileSlurmInfoFallback

Extends sams.loader.File but will use sacct to fetch data 
from slurmdbd if sams.sampler.SlurmInfo is not used or 
failes to fetch data.

Used by the [*sams-aggregator*](../sams-aggregator.md)

# Config options

Same options as sams.loader.File but extends with the following

## sacct

Path where sacct binary is located

Default: "/usr/bin/sacct"

## environment

Extra environtment for command.

An hash of key-value with envname-value.

Can for example be used to set the TZ option to get output in UTC.


# Example configuration

```
sams.loader.FileSlurmInfoFallback:
  in_path: /data/softwareaccounting/data
  archive_path: /data/softwareaccounting/archive
  error_path: /data/softwareaccounting/error
  file_pattern: '^.*\.json$'
  sacct: /usr/bin/sacct
  environment:
    TZ: UTC
```
