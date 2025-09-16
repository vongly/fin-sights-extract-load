import os, sys
from pathlib import Path
import json
import shutil
import glob
import boto3
import pandas as pd

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from env import (
    EXTRACT_DIR,
    PROJECT_DIRECTORY,
    S3_ACCESS_KEY,
    S3_SECRET_ACCESS_KEY,
    S3_REGION,
    S3_ENDPOINT_URL,
)


def print_pipeline_details(pipeline):
    pipeline_name = pipeline.pipeline_name
    destination = pipeline.destination.__class__.__name__.lower()
    dataset = pipeline.dataset_name
    working_dir = pipeline.pipelines_dir

    print('\n', ' RUNNING NEW PIPELINE -')
    print('  Name:', pipeline_name)
    if destination == 'filesystem':
        print('  Destination:', EXTRACT_DIR)
    else:
        print('  Destination:', destination)
    print('  Dataset:', dataset)
    print('  Working dir:', working_dir, '\n')

class AccessS3Bucket:
    def __init__(
        self,
        bucket_name='finsights-mvp-file-pickup',
        region_name=S3_REGION,
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
    ):
        
        self.bucket_name = bucket_name
        self.region_name = region_name
        self.endpoint_url = endpoint_url
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

        session = boto3.session.Session()

        self.client = session.client(
            's3',
            region_name=S3_REGION,
            endpoint_url=S3_ENDPOINT_URL,
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_ACCESS_KEY,
        )
    
    def get_file_structure(self, path):

        response = self.client.list_objects_v2(Bucket=self.bucket_name,Prefix=path, Delimiter='/')

        self.source_details = []
        sources = response.get('CommonPrefixes',None)

        for source in sources:
            ''' Source Level -> Lenders '''
            source_path = source['Prefix']
            source_name = source_path.split('/')[-2]

            response = self.client.list_objects_v2(Bucket=self.bucket_name, Prefix=source_path, Delimiter='/')
            source_file_type_paths = response.get('CommonPrefixes',None)

            '''
                Source Type Level -> Applications/Transactions
                Checks if anything exists
            '''
            if source_file_type_paths:
                source_type_paths = [ ft['Prefix'] for ft in source_file_type_paths ]
                source_types = [ ftp.split('/')[-2] for ftp in source_type_paths ]
    
                source_dictionary = {
                    'source': source_name,
                    'source_type_paths': source_type_paths,
                    'source_types': source_types,
                    }
                self.source_details.append(source_dictionary)

        return self.source_details

    def get_files(self, path):        
        response = self.client.list_objects_v2(Bucket=self.bucket_name,Prefix=path, Delimiter='/')
        if 'Contents' in response:
            file_paths = [ file['Key'] for file in response.get('Contents', None) ]
        else:
            file_paths = []
        return file_paths

    def create_folder(self, path, folder_name):
        folder_path = f'{path}{folder_name}/'

        try:
            self.client.head_object(Bucket=self.bucket_name, Key=folder_path)
        except:
            self.client.put_object(Bucket=self.bucket_name, Key=folder_path)

        return folder_path

    def move_file_test(self, path, target):
        print(f'  file path: {path}')
        print(f'target path: {target}')

    def move_file(self, path, target):

        self.client.copy_object(
            Bucket=self.bucket_name,
            CopySource={'Bucket': self.bucket_name, 'Key': path},
            Key=target,
        )

        self.client.delete_object(Bucket=self.bucket_name, Key=path)


if __name__ == '__main__':
    x = AccessS3Bucket()
    x.get_file_structure(path='_incoming_files/')

    x.get_files(
        path='_incoming_files/htf/transactions/',
    )