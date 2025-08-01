import os
from dotenv import load_dotenv

load_dotenv() 

DB_CONFIG = {
    'host': os.environ.get('DB_HOST', 'postgres'),
    'port': os.environ.get('DB_PORT', '5432'),
    'database': os.environ.get('DB_NAME', 'bank_etl'),
    'user': os.environ.get('DB_USER', 'ds_user'),
    'password': os.environ.get('DB_PASSWORD')
}