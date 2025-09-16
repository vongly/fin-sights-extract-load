import dlt
import duckdb
import s3fs

import sys
from pathlib import Path
import gzip, json
from glob import glob

import pandas as pd
from datetime import datetime, timezone

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from env import (
    S3_ACCESS_KEY,
    S3_ENDPOINT_URL,
    S3_SECRET_ACCESS_KEY,
)

# Identifies the filepaths of extracted data files
    ### need to sync/generalize file structure

class FileResource:
    def __init__(
            self,
            data_source,
            directory,
            s3_client=None,
    ):

        self.data_source = data_source
        self.directory = directory
        self.s3_client = s3_client
        self.processed = datetime.now(timezone.utc)
        self.processed_str = self.processed.strftime('%Y.%m.%d.%H.%M.%S')

        self.dfs = []        

    def query_s3_files(self):
        directory = self.directory
        files = self.s3_client.get_files(path=directory)

        self.bucket_path = 's3://' + self.s3_client.bucket_name + '/'
        
        if files != []:
            files = [ self.bucket_path + file for file in files ]

        self.files = files

        for file in files:
            if file.split('.')[-1] in ['xls','xlsx']:
                df = pd.read_excel(
                    file,
                    storage_options={
                        'key': S3_ACCESS_KEY,
                        'secret': S3_SECRET_ACCESS_KEY,
                        'client_kwargs': {'endpoint_url': S3_ENDPOINT_URL},
                    }
                )
                df['processed'] = self.processed
                self.dfs.append(df)
            elif file.split('.')[-1] in ['csv']:
                df = pd.read_csv(
                    file,
                    storage_options={
                        'key': S3_ACCESS_KEY,
                        'secret': S3_SECRET_ACCESS_KEY,
                        'client_kwargs': {'endpoint_url': S3_ENDPOINT_URL},
                    }
                )
                df['processed'] = self.processed
                self.dfs.append(df)

    def query_local_files(self):
        directory = Path(self.directory)

        if directory.exists():
            files = glob(f'{directory}/*')
        else:
            files = []

        self.files = files

        for file in files:
            if file.split('.')[-1] in ['xls','xlsx']:
                df = pd.read_excel(file)
                df['processed'] = self.processed
                self.dfs.append(df)
            elif file.split('.')[-1] in ['csv']:
                df = pd.read_csv(file)
                df = pd.read_excel(file)
                df['processed'] = self.processed
                self.dfs.append(df)

    def yield_file_results(self):
        if self.dfs != []:
            for df in self.dfs:
                for record in df.to_dict(orient='records'):
                    yield record
            
    # Generates a resource from file
    # creates one resource for one source
    def create_resource(self,**kwargs):
        table_suffix = kwargs.get('table_suffix', '')
        table_name = self.data_source + table_suffix

        @dlt.resource(name=self.data_source, table_name=table_name, write_disposition='append')
        def my_resource():
            yield from self.yield_file_results()
        return my_resource

if __name__ == '__main__':
    pass