
import os
import pandas as pd
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import time
# Nuevos imports para pyathena
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor

# Load environment variables
load_dotenv()

# AWS Configuration
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_region = os.getenv('AWS_DEFAULT_REGION')

# RDS Database Configuration
db_config = {
    'dbname': os.getenv('PROD_POSTGRES_DB'),
    'user': os.getenv('PROD_POSTGRES_USER'),
    'host': os.getenv('PROD_POSTGRES_HOST'),
    'port': os.getenv('PROD_POSTGRES_PORT')
}

# Athena Configuration
ATHENA_DATABASE = os.getenv('ATHENA_DATABASE')
ATHENA_OUTPUT_BUCKET = os.getenv('ATHENA_OUTPUT_BUCKET')
ATHENA_OUTPUT_PREFIX = os.getenv('ATHENA_OUTPUT_PREFIX')

def get_rds_password():
    client = boto3.client('rds',
                          region_name=aws_region,
                          aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key=aws_secret_access_key)

    try:
        token = client.generate_db_auth_token(
            DBHostname=db_config['host'],
            Port=db_config['port'],
            DBUsername=db_config['user'],
            Region=aws_region
        )
        return token
    except ClientError as e:
        print(f"Error generating authentication token: {e}")
        return None

# Get authentication token for RDS
password = get_rds_password()

# Create SQLAlchemy connection URL for RDS
DATABASE_URL = f"postgresql://{db_config['user']}:{password}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"

# Create SQLAlchemy engine for RDS
engine = create_engine(DATABASE_URL, echo=True, client_encoding='utf8')

# Create Athena client
athena_client = boto3.client('athena',
                             region_name=aws_region,
                             aws_access_key_id=aws_access_key_id,
                             aws_secret_access_key=aws_secret_access_key)


def ejecutar_query_rds(query, start_date=None, end_date=None):
    try:
        formatted_query = query.format(start_date=start_date, end_date=end_date)
        with engine.connect() as connection:
            result = pd.read_sql_query(formatted_query, connection)
        return result
    except Exception as e:
        print(f"Error executing RDS query: {e}")
        return None


# ... (imports y código anterior)

def athena_query(query, start_date=None, end_date=None):
    """
    Ejecuta una consulta en Athena con fechas opcionales.

    Args:
        query (str): Query SQL a ejecutar
        start_date (str, optional): Fecha de inicio en formato YYYY-MM-DD
        end_date (str, optional): Fecha de fin en formato YYYY-MM-DD

    Returns:
        pandas.DataFrame: Resultados de la consulta
    """
    try:
        # Si ambas fechas están presentes, formatea la query
        if start_date is not None and end_date is not None:
            formatted_query = query.format(start_date=start_date, end_date=end_date)
        else:
            # Si no hay fechas, usa la query tal cual
            formatted_query = query

        print("Query final:", formatted_query)

        # Crear conexión a Athena usando pyathena
        conn = connect(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region,
            s3_staging_dir=f's3://{ATHENA_OUTPUT_BUCKET}/{ATHENA_OUTPUT_PREFIX}',
            schema_name=ATHENA_DATABASE
        )

        # Usar pd.read_sql directamente
        df = pd.read_sql(formatted_query, conn)
        print(f"Total de registros recuperados: {len(df)}")
        return df

    except Exception as e:
        print(f"Error executing Athena query: {e}")
        return None

# ... (resto del código)
# def athena_query(query, start_date, end_date):
#     try:
#         formatted_query = query.format(start_date=start_date, end_date=end_date)
#         response = athena_client.start_query_execution(
#             QueryString=formatted_query,
#             QueryExecutionContext={'Database': ATHENA_DATABASE},
#             ResultConfiguration={'OutputLocation': f's3://{ATHENA_OUTPUT_BUCKET}/{ATHENA_OUTPUT_PREFIX}'}
#         )
#
#         query_execution_id = response['QueryExecutionId']
#
#         while True:
#             query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
#             status = query_status['QueryExecution']['Status']['State']
#             if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
#                 break
#             time.sleep(1)
#
#         if status == 'SUCCEEDED':
#             results = athena_client.get_query_results(
#                 QueryExecutionId=query_execution_id,
#                 MaxResults=5000000  # Aumentamos el límite
#             )
#             columns = [col['Name'] for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
#             data = []
#             for row in results['ResultSet']['Rows'][1:]:
#                 data.append([field.get('VarCharValue', '') for field in row['Data']])
#             return pd.DataFrame(data, columns=columns)
#         else:
#             error_details = query_status['QueryExecution']['Status'].get('AthenaError', {})
#             error_message = error_details.get('ErrorMessage', 'No error message available')
#             error_type = error_details.get('ErrorCategory', 'Unknown error type')
#             print(f"Athena query failed with status: {status}")
#             print(f"Error Type: {error_type}")
#             print(f"Error Message: {error_message}")
#             return None
#     except Exception as e:
#         print(f"Error executing Athena query: {e}")
#         return None