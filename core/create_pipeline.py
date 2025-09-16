import dlt
import duckdb

import sys
from pathlib import Path
import gzip
import re
import json

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from env import (
    PIPELINES_DIR,
)

from utils.helpers import (
    print_pipeline_details,
)

class Pipeline:
    def __init__(
        self,
        pipeline_name: str,
        destination: str,
        dataset: str,
        resources: object,
        pipelines_dir=PIPELINES_DIR,
        **kwargs,
    ):

        self.pipeline_object = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination=destination,
            dataset_name=dataset,
            pipelines_dir=pipelines_dir,
        )
        
        self.resources = resources if isinstance(resources, list) else [resources]

    def run_pipeline(self):
        print_pipeline_details(self.pipeline_object)
        jobs = []

        load_info = self.pipeline_object.run(self.resources, loader_file_format='csv')
        load_packages = load_info.load_packages

        for package in load_packages:
            completed_jobs = package.jobs.get('completed_jobs', [])
            failed_jobs = package.jobs.get('failed_jobs', [])

            for job in completed_jobs + failed_jobs:
                job_details = {}
                job_details['table'] = getattr(job.job_file_info, 'table_name', None)
                job_details['state'] = getattr(job, 'state', None)
                job_details['file_path'] = getattr(job, 'file_path', None)
                job_details['MB'] = round(getattr(job, 'file_size', None)/1000000, 2) if getattr(job, 'file_size', None) else None
                job_details['seconds_elapsed'] = round(getattr(job, 'elapsed', None), 2) if getattr(job, 'elapsed', None) else None
                job_details['file_id'] = getattr(job, 'file_id', None)

                
                if job.file_path.endswith('.parquet'):
                    job_details['records'] = duckdb.sql(f'''select count(*) from read_parquet('{job.file_path}')''').fetchall()[0][0]
                elif job.file_path.endswith('.jsonl.gz'):
                    job_details['records'] = duckdb.sql(f'''select count(*) from read_json('{job.file_path}')''').fetchall()[0][0]
                elif job.file_path.endswith('.insert_values.gz'):
                    with gzip.open(job.file_path, "rt", encoding="utf-8") as f:
                        text = f.read()
                    job_details['records'] = len(re.findall(r'\([^\)]*\)', text))

                jobs.append(job_details)

        self.jobs = sorted(jobs, key=lambda x: x['table'])
        self.jobs_json = json.dumps(self.jobs, indent=2)

