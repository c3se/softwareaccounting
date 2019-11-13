"""
"""
import os
import re
import json

import sams.base

import logging
logger = logging.getLogger(__name__)

class Loader(sams.base.Loader):
    
    def __init__(self,id,config):
        super(Loader,self).__init__(id,config)
        self.in_path = self.config.get([self.id,'in_path'])
        self.archive_path = self.config.get([self.id,'archive_path'])
        self.error_path = self.config.get([self.id,'error_path'])
        self.file_pattern = re.compile(self.config.get([self.id,'file_pattern'],"^.*$"))
        self.files = []
        self.current_file = None

    def load(self):
        """ Find files in in_path matching file_pattern """
        for root, dirs, files in os.walk(self.in_path):
            for file in files:
                logger.debug("Found file: %s", file)
                if self.file_pattern.match(file):
                    logger.debug("Add %s to files[]" % os.path.join(root,file))
                    self.files.append({
                        'file': file,
                        'path': os.path.relpath(root,self.in_path)
                    })
    
    def next(self):
        if len(self.files) == 0:
            return None            
        self.current_file = self.files[0]
        self.files = self.files[1:]
        logger.debug(self.current_file)
        filename = os.path.join(self.in_path,self.current_file['path'],self.current_file['file'])
        try:
            with open(filename,"r") as file:
                return json.load(file)
        except IOError as e:
            logger.error("Failed to load: %s", filename)
        except ValueError as e:
            logger.error("Failed to decode JSON in file: %s", filename)
        return None

    def error(self):
        """ move file from in_path -> error_path/ """
        logger.info("Error: %s" % os.path.join(self.current_file['path'],self.current_file['file']))

        out_path = os.path.join(self.error_path,self.current_file['path'])
        if not os.path.isdir(out_path):
            try:
                os.mkdir(out_path)
            except IOError as err:
                # Handle possible raise from other process
                if not os.path.isdir(out_path):
                    assert False, "Failed to mkdir '%s' " % out_path

        # Rename file to error directory
        os.rename(os.path.join(self.in_path,self.current_file['path'],self.current_file['file']),
                  os.path.join(out_path,self.current_file['file']))

        self.current_file = None
        pass

    def commit(self):
        """ move file from in_path -> archive_path/ """
        logger.info("Commit: %s" % os.path.join(self.current_file['path'],self.current_file['file']))

        out_path = os.path.join(self.archive_path,self.current_file['path'])
        if not os.path.isdir(out_path):
            try:
                os.mkdir(out_path)
            except IOError as err:
                # Handle possible raise from other process
                if not os.path.isdir(out_path):
                    assert False, "Failed to mkdir '%s' " % out_path

        # Rename file to archive directory
        os.rename(os.path.join(self.in_path,self.current_file['path'],self.current_file['file']),
                  os.path.join(out_path,self.current_file['file']))

        self.current_file = None
        pass
