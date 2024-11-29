import os
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError
from sqlalchemy import create_engine, text

# Cargar variables de entorno
load_dotenv()

# Configuración AWS
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_region = os.getenv('AWS_DEFAULT_REGION')

# Configuración Base de datos
db_config = {
    'dbname': os.getenv('PROD_POSTGRES_DB'),
    'user': os.getenv('PROD_POSTGRES_USER'),
    'host': os.getenv('PROD_POSTGRES_HOST'),
    'port': os.getenv('PROD_POSTGRES_PORT')
}


def get_rds_password():
    """Obtiene el token de autenticación de AWS RDS"""
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
        print(f"Error generando token de autenticación: {e}")
        return None


def check_database_access():
    try:
        print("\n=== Diagnóstico de Base de Datos ===")

        # 1. Crear conexión
        password = get_rds_password()
        if not password:
            raise Exception("No se pudo obtener el token de autenticación")

        connection_string = f"postgresql://{db_config['user']}:{password}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
        engine = create_engine(connection_string)
        print("✓ Conexión establecida exitosamente")

        with engine.connect() as connection:
            # 2. Información básica
            print("\n=== Información de Usuario y Base de Datos ===")
            result = connection.execute(text("""
                SELECT current_user, current_database(), session_user, version();
            """))
            user_info = result.fetchone()
            print(f"Usuario actual: {user_info[0]}")
            print(f"Base de datos: {user_info[1]}")
            print(f"Usuario de sesión: {user_info[2]}")
            print(f"Versión PostgreSQL: {user_info[3]}")

            # 3. Obtener lista exacta de esquemas
            print("\n=== Esquemas Visibles ===")
            schemas_query = text("""
                SELECT DISTINCT 
                    n.nspname AS schema_name,
                    pg_catalog.pg_get_userbyid(n.nspowner) AS schema_owner
                FROM pg_catalog.pg_namespace n
                WHERE n.nspname NOT LIKE 'pg_%'
                    AND n.nspname != 'information_schema'
                ORDER BY schema_name;
            """)
            schemas = connection.execute(schemas_query).fetchall()

            for schema in schemas:
                schema_name = schema[0]
                print(f"\nEsquema: {schema_name}")
                print(f"Propietario: {schema[1]}")

                try:
                    print("Verificando permisos...")
                    # Obtener lista exacta de tablas
                    tables_query = text("""
                        SELECT 
                            table_name,
                            table_type
                        FROM information_schema.tables 
                        WHERE table_schema = :schema;
                    """)
                    tables = connection.execute(tables_query, {'schema': schema_name}).fetchall()
                    print(f"Tablas visibles: {len(tables)}")

                    if tables:
                        print("\nPrimeras 5 tablas y acceso:")
                        for i, (table_name, table_type) in enumerate(tables):
                            if i >= 5:  # Limitar a 5 tablas
                                break
                            try:
                                # Intentar acceder a la tabla
                                access_query = text(f"""
                                    SELECT 1 FROM "{schema_name}"."{table_name}" LIMIT 1;
                                """)
                                connection.execute(access_query)
                                print(f"  ✓ {table_name} ({table_type}) - Accesible")
                            except Exception as e:
                                print(f"  ✗ {table_name} ({table_type}) - No accesible: {str(e)[:100]}")

                    # Verificar permisos explícitos
                    perms_query = text("""
                        SELECT DISTINCT privilege_type
                        FROM information_schema.table_privileges 
                        WHERE table_schema = :schema 
                        AND grantee = current_user;
                    """)
                    perms = connection.execute(perms_query, {'schema': schema_name}).fetchall()

                    if perms:
                        print("\nPermisos explícitos:")
                        for perm in perms:
                            print(f"  - {perm[0]}")
                    else:
                        print("\nNo se encontraron permisos explícitos")

                except Exception as e:
                    print(f"Error verificando esquema: {str(e)}")

    except Exception as e:
        print(f"Error en el diagnóstico: {str(e)}")
    finally:
        if 'engine' in locals():
            engine.dispose()


if __name__ == "__main__":
    check_database_access()