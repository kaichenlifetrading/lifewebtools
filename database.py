import os
import pandas as pd
from .mapping import *


class Database(Mapping):
    def __init__(self):
        super().__init__()

    @staticmethod
    def check_file(directory, filename):
        return filename in os.listdir(directory)

    @staticmethod
    def generate_filename(dbtype, exchange, date, session, **kwargs):
        saved_args = locals()
        expanded_args = {k: v for k, v in saved_args.items() if k not in ('dbtype', 'kwargs')}
        expanded_args.update(kwargs)
        message_types_str = ''
        for message_type in expanded_args.values():
            message_types_str += '$' + message_type
        return dbtype + message_types_str

    @staticmethod
    def arguments_filename(filename):
        arg_list = ['dbtype', 'exchange', 'date', 'session', 'contract', 'kwargs']
        split_list = filename.split('$')
        return {k: v for k, v in zip(arg_list, split_list)}

    def store_pickle(self, directory, filename, df):
        df.to_pickle(directory + filename, compression=self.compression_type)

    def load_pickle(self, directory, filename):
        return pd.read_pickle(directory + filename, compression=self.compression_type)

    def directory_list(self, dbtype):
        return os.listdir(self.address[dbtype + '_address'])



