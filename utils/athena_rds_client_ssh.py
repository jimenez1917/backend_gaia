import os
import pandas as pd
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Load environment variables
load_dotenv()


def get_rds_password():
    """
    Gets temporary authentication token from AWS
    """
    try:
        client = boto3.client('rds',
                              region_name=os.getenv('AWS_DEFAULT_REGION'),
                              aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                              aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
                              )

        token = client.generate_db_auth_token(
            DBHostname=os.getenv('SSH_POSTGRES_HOST_URL'),  # Use the actual RDS endpoint
            Port=5432,  # RDS port (not the SSH tunnel port)
            DBUsername=os.getenv('SSH_POSTGRES_USER'),
            Region=os.getenv('AWS_DEFAULT_REGION')
        )
        return token
    except ClientError as e:
        print(f"Error generating authentication token: {e}")
        return None


def get_db_connection():
    """
    Creates database connection using SSH tunnel and AWS IAM authentication
    """
    try:
        if os.getenv('ENV') == 'SSH':
            # Get temporary authentication token
            password = get_rds_password()

            if not password:
                raise ValueError("Could not generate RDS authentication token")

            # Use SSH tunnel configuration
            db_config = {
                'user': os.getenv('SSH_POSTGRES_USER'),
                'password': password,  # Using AWS temporary token
                'host': 'localhost',  # Connect through SSH tunnel
                'port': os.getenv('SSH_POSTGRES_PORT', '5433'),
                'dbname': os.getenv('SSH_POSTGRES_DB')
            }

            # Construct connection URL
            DATABASE_URL = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"

            # Create SQLAlchemy engine
            engine = create_engine(
                DATABASE_URL,
                echo=False,
                client_encoding='utf8'
            )

            return engine
        else:
            raise ValueError("ENV must be set to 'SSH' in .env file")

    except Exception as e:
        print(f"Error creating database connection: {e}")
        raise


def ejecutar_query_rds(query, start_date=None, end_date=None):
    """
    Executes a query on RDS through SSH tunnel
    """
    try:
        # Get database engine
        engine = get_db_connection()

        # Format query if dates are provided
        formatted_query = query.format(start_date=start_date, end_date=end_date)

        # Execute query
        with engine.connect() as connection:
            result = pd.read_sql_query(formatted_query, connection)

        return result

    except SQLAlchemyError as e:
        print(f"Database error: {e}")
        return None
    except Exception as e:
        print(f"Error executing RDS query: {e}")
        return None