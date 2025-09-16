import os
from pathlib import Path
from dotenv import load_dotenv

env_path = Path(__file__).resolve().parents[0] / '.env_dev'
load_dotenv(dotenv_path=env_path)

PROJECT_DIRECTORY = os.path.dirname(os.path.abspath(__file__))

POSTGRES_USER = os.environ['POSTGRES_USER']
POSTGRES_PASSWORD = os.environ['POSTGRES_PASSWORD']
POSTGRES_HOST = os.environ['POSTGRES_HOST']
POSTGRES_PORT = os.environ['POSTGRES_PORT']
POSTGRES_DB = os.environ['POSTGRES_DB']

PIPELINES_DIR_RELATIVE = os.environ['PIPELINES_DIR_RELATIVE']
PIPELINES_DIR = Path(__file__).parent / PIPELINES_DIR_RELATIVE
INCOMIONG_DIR = os.environ['INCOMIONG_DIR']
EXTRACT_DIR = os.environ['EXTRACT_DIR']

DROPBOX_KEY = os.environ['DROPBOX_KEY']
DROPBOX_SECRET = os.environ['DROPBOX_SECRET']

S3_USERNAME = os.environ['S3_USERNAME']
S3_ACCESS_KEY = os.environ['S3_ACCESS_KEY']
S3_SECRET_ACCESS_KEY = os.environ['S3_SECRET_ACCESS_KEY']
S3_REGION = os.environ['S3_REGION']
S3_BUCKET_URL = os.environ['S3_BUCKET_URL']
S3_ENDPOINT_URL = os.environ['S3_ENDPOINT_URL']

if __name__ == '__main__':
    for var_name, value in list(locals().items()):
        print(f"{var_name} = {value}")
    print('\n')
