""" Base class for socket listening-based demand-driven output. """
from abc import ABC, abstractmethod
from sams.core import Config
from typing import Iterable
import logging
import os
import select
import socket
import threading

logger = logging.getLogger(__name__)


class Listener(ABC):
    """ Base class for listening to sockets and sending encoded data. """
    def __init__(self,
                 class_path: str,
                 config: Config,
                 samplers: Iterable):
        self.class_path = class_path
        self.config = config
        self.samplers = samplers
        socket_family = self.config.get([self.class_path, 'socket_family'],
                                        socket.AF_UNIX)
        socket_type = self.config.get([self.class_path, 'socket_type'],
                                      socket.SOCK_STREAM)
        protocol_number = self.config.get([self.class_path, 'socket'],
                                          0)
        file_number = self.config.get([self.class_path, 'file_number'],
                                      None)
        socket_directory = self.config.get([self.class_path, 'socketdir'],
                                           '/tmp/softwareaccounting')
        self.job_id = self.config.get(['options', 'jobid'],
                                      0)
        self.server_socket = socket.socket(socket_family,
                                           socket_type,
                                           protocol_number,
                                           file_number)
        self.is_finished = False
        if not os.path.isdir(socket_directory):
            os.mkdir(socket_directory)
        self.socket_path = f'{socket_directory}/{self.class_path}_{self.job_id:d}.socket'
        logger.debug(f'Socket path: {self.socket_path}')
        if os.path.exists(self.socket_path):
            os.unlink(self.socket_path)
        self.server_socket.bind(self.socket_path)
        self.thread = threading.Thread(target=self._listen)

    def start(self):
        """ Starts listening on thread. """
        self.thread.start()
        logger.debug('Launching listening thread...')

    def exit(self):
        """ Sets finished flag to True to kill thread, joins and cleans up. """
        if not self.is_finished:
            self.is_finished = True
            if self.thread.is_alive:
                self.thread.join(1)
            self.server_socket.close()
            os.unlink(self.socket_path)

    def _listen(self):
        """Listens to the socket for connections."""
        self.server_socket.listen(5)
        while not self.is_finished:
            logger.debug('Waiting for connections...')
            readable, _, _ = select.select([self.server_socket], [], [], 1)
            if len(readable) > 0:
                try:
                    connection, address = self.server_socket.accept()
                    logger.debug(f'Connected to {address}')
                except Exception as e:
                    logger.debug(f'Sending data failed due to {e}!')
                with connection:
                    try:
                        connection.sendall(self.encoded_data)
                    except Exception as e:
                        logger.debug(f'Sending data to {address} failed due to {e}!')

    @property
    @abstractmethod
    def encoded_data(self) -> str:
        """ Encoded data to be sent to client. """
        raise NotImplementedError
