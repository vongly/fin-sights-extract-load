import dlt

import sys
import os
from pathlib import Path

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from core import create_pipeline
from utils.resources.excel_file_pre_formatted import FileResource
from utils.helpers import AccessS3Bucket

from env import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    POSTGRES_DB,
)

os.environ['DESTINATION__POSTGRES__CREDENTIALS__HOST'] = POSTGRES_HOST
os.environ['DESTINATION__POSTGRES__CREDENTIALS__PORT'] = POSTGRES_PORT
os.environ['DESTINATION__POSTGRES__CREDENTIALS__USERNAME'] = POSTGRES_USER
os.environ['DESTINATION__POSTGRES__CREDENTIALS__PASSWORD'] = POSTGRES_PASSWORD
os.environ['DESTINATION__POSTGRES__CREDENTIALS__DATABASE'] = POSTGRES_DB
os.environ['DESTINATION__POSTGRES__CREDENTIALS__SSLMODE'] = 'verify-full'

class ProcessPipeline:

    def run_pipeline(self, resource: object, resource_name):
        
        pipeline_name = f'fin_sight_source_{resource_name}'
        destination = 'postgres'
        dataset = 'raw_sources'

        self.pipeline = create_pipeline.Pipeline(
            pipeline_name=pipeline_name,
            destination=destination,
            dataset=dataset,
            resources=resource,
        )

        self.pipeline.run_pipeline()

        return self.pipeline

    def print_results(self):
        print(self.pipeline.jobs_json)

    def check_completion(self):
        if self.pipeline.jobs:
            state = self.pipeline.jobs[0]['state']

            if state == 'completed_jobs':
                return True
            else:
                return False
        else:
            return False

if __name__ == '__main__':

    # Checks file structure at bucket path -> will interpret now sources
    client = AccessS3Bucket()
    bucket_base_path = '_incoming_files/'
    sources_details = client.get_file_structure(path=bucket_base_path)

    # Iterates through file structure
    for sd in sources_details:
        source = sd['source']
        for source_type in sd['source_types']:
            directory = f'{bucket_base_path}{source}/{source_type}/'

            # Creates Resource Object
            resource_obj = FileResource(
                data_source=source,
                directory=directory,
                s3_client=client,
            )
            resource_obj.query_s3_files()
            resource = resource_obj.create_resource(table_suffix = '_' + source_type)

            # Creates Pipeline Object
            pipeline = ProcessPipeline()
            pipeline.run_pipeline(
                resource=resource,
                resource_name=source,
            )
            pipeline.print_results()

            # Checks for Completion
            if pipeline.check_completion() is True:
                processed_folder = 'processed/'
                
                # Creates Processed Folder
                client.create_folder(
                    path=directory,
                    folder_name=processed_folder,
                )
                
                # Moves completed file to process folder
                for i, file in enumerate(resource_obj.files):
                    file_type = file.split('.')[-1]
                    client.move_file_test(
                        path = f'{file}',
                        target = f'{resource_obj.bucket_path}{resource_obj.directory}{processed_folder}{resource_obj.processed_str}-{i}.{file_type}'
                    )
