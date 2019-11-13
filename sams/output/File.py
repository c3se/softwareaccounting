"""
Write outputs into the file system

Config Options:

sams.output.File:
  # Path where the output files are written.
  base_path: /scratch/softwareaccounting/data

  # Write files in file pattern. 
  # Available data for replace is: jobid & node
  file_pattern: "%(jobid)s.%(node)s.json"

  # "Hash" the output based on --jobid / jobid_hash_size
  jobid_hash_size: 1000

  # If set uses the sysfsuid() syscall to write files as uid.
  # This does not work on lustre.
  # write_as_uid: 2066

  # Skip the list of modules.
  exclude: ['sams.sampler.ModuleName']
"""
import json
import logging
import os

import sams.base
import sams.setfsuid

logger = logging.getLogger(__name__)

class Output(sams.base.Output):
    """ File output Class """

    def __init__(self,id,config):
        super(Output,self).__init__(id,config)
        self.exclude = dict((e, True) for e in self.config.get([self.id,"exclude"],[]))
        self.data = {}

    def store(self,data):
        for k,v in data.items():
            if k in self.exclude:
                continue
            self.data[k] = v    

    def write(self):
        base_path=self.config.get([self.id,"base_path"],"/tmp")
        file_pattern = self.config.get([self.id,"file_pattern"],"%(jobid).%(node).json")
        jobid = self.config.get(['options','jobid'],0)
        node  = self.config.get(['options','node'],0)
        jobid_hash_size = self.config.get([self.id,'jobid_hash_size'])
        write_as_uid = self.config.get([self.id,'write_as_uid'])

        if write_as_uid is not None:
            logger.error(sams.setfsuid.setfsuid(write_as_uid))

        filename = file_pattern % { 'jobid': jobid, 'node': node }
        tfilename = ".%s" % filename

        if jobid_hash_size is not None:
            base_path = os.path.join(base_path,str(int(jobid / jobid_hash_size)))

            if not os.path.isdir(base_path):
                try:
                    os.mkdir(base_path)
                except IOError as err:
                    # Handle possible raise from other process
                    if not os.path.isdir(base_path):
                        assert False, "Failed to mkdir '%s' " % base_path

        try:
            with open(os.path.join(base_path,tfilename),"w") as file:
                file.write(json.dumps(self.data,sort_keys=True,separators=(',',':')))
            os.rename(os.path.join(base_path,tfilename),os.path.join(base_path,filename))
        except IOError as err:            
            logger.debug("Failed to write file")
            try:
                os.unlink(os.path.join(base_path,tfilename))
            except IOError as err:
                logger.error("Failed to unlink tmp file")
                # ignore unlink failure
                pass

