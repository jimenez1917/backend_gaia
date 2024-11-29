import os
import pandas as pd
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import time
from sshtunnel import SSHTunnelForwarder
import logging

# Configurar logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# AWS Configuration
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_region = os.getenv('AWS_DEFAULT_REGION')

# RDS Database Configuration
db_config = {
    'dbname': os.getenv('PROD_POSTGRES_DB', 'db_sisc_pg'),
    'user': os.getenv('PROD_POSTGRES_USER', 'AIDATJPAD2GIWAEBRXJJY'),
    'host': os.getenv('PROD_POSTGRES_HOST', 'etl-semanal.cq9aev6wh2p5.us-east-1.rds.amazonaws.com'),
    'local_port': 5433,
    'remote_port': int(os.getenv('PROD_POSTGRES_PORT', '5432'))
}


def get_rds_password():
    """Genera un token de autenticación para RDS usando AWS IAM"""
    client = boto3.client('rds',
                          region_name=aws_region,
                          aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key=aws_secret_access_key)

    try:
        token = client.generate_db_auth_token(
            DBHostname=db_config['host'],
            Port=db_config['remote_port'],
            DBUsername=db_config['user'],
            Region=aws_region
        )
        return token
    except ClientError as e:
        logger.error(f"Error generating authentication token: {e}")
        return None


class DatabaseConnection:
    def __init__(self):
        self.tunnel = None
        self.engine = None

    def __enter__(self):
        try:
            logger.debug("Intentando establecer túnel SSH...")

            # Obtener el token de autenticación de AWS IAM
            auth_token = get_rds_password()
            if not auth_token:
                raise Exception("No se pudo obtener el token de autenticación de AWS")

            logger.debug(f"Token de autenticación generado correctamente")

            # Configurar el túnel SSH
            self.tunnel = SSHTunnelForwarder(
                (db_config['host'], 22),
                ssh_username=db_config['user'],
                remote_bind_address=(db_config['host'], db_config['remote_port']),
                local_bind_address=('0.0.0.0', db_config['local_port'])
            )

            logger.debug("Iniciando túnel SSH...")
            self.tunnel.start()

            logger.debug(
                f"Túnel SSH establecido - Local: {db_config['local_port']}, Remoto: {db_config['remote_port']}")

            # Crear la URL de conexión usando el túnel SSH y el token de autenticación
            db_url = f"postgresql://{db_config['user']}:{auth_token}@localhost:{db_config['local_port']}/{db_config['dbname']}"
            logger.debug("URL de conexión creada (no mostrada por seguridad)")

            self.engine = create_engine(db_url, echo=True, client_encoding='utf8')

            return self.engine

        except Exception as e:
            logger.error(f"Error detallado al establecer la conexión: {str(e)}", exc_info=True)
            if self.tunnel and self.tunnel.is_active:
                self.tunnel.close()
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.tunnel and self.tunnel.is_active:
            logger.debug("Cerrando túnel SSH")
            self.tunnel.close()


def ejecutar_query_rds(query, start_date=None, end_date=None):
    try:
        if start_date and end_date:
            formatted_query = query.format(start_date=start_date, end_date=end_date)
        else:
            formatted_query = query

        logger.debug(f"Ejecutando query RDS: {formatted_query}")

        with DatabaseConnection() as engine:
            with engine.connect() as connection:
                result = pd.read_sql_query(formatted_query, connection)
        return result
    except Exception as e:
        logger.error(f"Error executing RDS query: {e}", exc_info=True)
        return None


if __name__ == "__main__":
    # Ejemplo consulta RDS
    query_rds = "SELECT * FROM aseco_view LIMIT 5"
    result_rds = ejecutar_query_rds(query_rds)
    if result_rds is not None:
        print("RDS Result:", result_rds)