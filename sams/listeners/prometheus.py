import logging
import re
from typing import Iterable, Dict, List
from sams.core import Config
from .base_listener import Listener as BaseListener

logger = logging.getLogger(__name__)


class Listener(BaseListener):
    """
    Listener for Prometheus output.
    """
    def __init__(self,
                 class_path: str,
                 config: Config,
                 samplers: List):
        super().__init__(class_path,
                         config,
                         samplers)
        self.static_map = self.config.get([self.class_path, "static_map"], {})
        self.map = self.config.get([self.class_path, "map"], {})
        self.metrics = self.config.get([self.class_path, "metrics"], {})

    @staticmethod
    def _nested_getitem(dct: Dict, keys: Iterable) -> Dict:
        """ Fetches an item from nested dictionaries.
        If any of the keys in `keys` is missing, this method
        returns `None`."""
        for key in keys:
            if key in dct:
                dct = dct[key]
            else:
                return None
        return dct

    def _flatten_dict(self, dct, base='') -> List:
        """ Recursively flattens a dictionary into a list-of-dictionaries,
        with each dictionary in the list possessing the entries `'match'`
        and `'value'`."""
        out = []
        for key in dct.keys():
            nb = "/".join([base, key])
            if key in dct and isinstance(dct[key], dict):
                out = out + self._flatten_dict(dct[key], base=nb)
            else:
                out = out + [{"match": nb, "value": dct[key]}]
        return out

    @property
    def encoded_data(self) -> bytes:
        """ The most recent data from attached samplers,
        formatted, compiled and encoded to UTF-8 format. """
        data = self._get_all_samples()
        # Then match config with entries and create flattened, compiled data
        compiled_data = dict()
        for match_set in self._get_matching_entries(data):
            self._compile_data(data,
                               compiled_data,
                               *match_set)
        # Parse & encode compiled data into bytestring.
        return self._get_bytestring(compiled_data)

    def _get_all_samples(self) -> Dict:
        """ Returns compilation of all most recent samples
        as a dictionary ``{sample['id']: sample[v'alue']}``
        Skips ``None`` entries.
        """
        data = dict()
        for sampler in self.samplers:
            if sampler.most_recent_sample is not None:
                for s in sampler.most_recent_sample:
                    # if id repeated, update data dictionary
                    if s['id'] in data:
                        data[s['id']].update(s['data'])
                    else:
                        data.update({s['id']: s['data']})
        return data

    def _get_matching_entries(self,
                              data: Dict) -> Iterable:
        flat_dictionary = self._flatten_dict(data)
        logger.debug(f'Flat dictionary: {flat_dictionary}')
        for d in flat_dictionary:
            for metric, destination in self.metrics.items():
                reg = re.compile(metric)
                m = reg.match(d['match'])
                logger.debug(f'Metric matches: {m}')
                if m is not None:
                    yield (str(d['value']),
                           destination,
                           m.groupdict())

    @staticmethod
    def _get_bytestring(compiled_data: Dict) -> bytes:
        """ Takes the compiled data, parses entry names, and
        turns it into a Prometheus-formatted bytestring,
        ready to be sent over socket. """
        helped = dict()
        metric_re = re.compile(r'^(\S+){')
        formatted_data = []
        for m in sorted(compiled_data.keys()):
            logger.debug(f'Data key: {m}')
            match = metric_re.match(m)
            # Check if key is in config
            if match is None:
                continue
            if match.group(1) not in helped:
                helped[match.group(1)] = True
                formatted_data.append(f'# HELP {match.group(1):s} Job Usage Metrics')
                formatted_data.append(f'# TYPE {match.group(1):s} gauge')
            v = compiled_data[m]
            formatted_data.append(f'{m} {str(v)}')
        data_bytestring = '\n'.join(formatted_data).encode('utf-8')
        return data_bytestring

    def _compile_data(self,
                      in_data: Dict,
                      out_data: Dict,
                      value,
                      destination: str,
                      extra_mappings: Dict) -> None:
        """ Subroutine method that updates `out_data`
        with entries from `in_data` based on the provided
        mappings. """
        # static_map is per-node, constant metadata
        d = self.static_map.copy()
        d.update(extra_mappings)
        # Get per-job metadata
        for k, v in self.map.items():
            m = self._nested_getitem(in_data, v.split('/'))
            if m is None:
                logger.warning(f'map: {k}: {v} is missing')
                return
            d[k] = m
        try:
            # Parse metadata into output string
            dest = destination % d
        except Exception as e:
            logger.error(e)
            return
        if len(value) == 0:
            logger.warning(f'{dest} got no metric')
        # Store output string as key and data as value
        out_data[dest] = value
        logger.debug(f'Storing {dest} = {str(value)}')
