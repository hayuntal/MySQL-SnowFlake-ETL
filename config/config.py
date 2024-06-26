from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Get Root Path
ROOT_PATH = os.path.dirname(os.path.dirname(__file__))

# Define global variables of MY SQL SERVER
SQL_CLIENT_HOST = os.getenv('MY_SQL_HOST')
SQL_CLIENT_USER = os.getenv('SQL_CLIENT_USER')
SQL_CLIENT_PASSWORD = os.getenv('SQL_CLIENT_PASSWORD')
SQL_CLIENT_DATABASE = os.getenv('SQL_CLIENT_DATABASE')

# Define global variables of SnowFlake
SF_USER = os.getenv('SF_USER')
SF_PASSWORD = os.getenv('SF_PASSWORD')
SF_ACCOUNT = os.getenv('SF_ACCOUNT')
SF_WAREHOUSE = os.getenv('SF_WAREHOUSE')
SF_DB = os.getenv('SF_DB')
SF_SCHEMA = os.getenv('SF_SCHEMA')