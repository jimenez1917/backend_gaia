import pandas as pd
import numpy as np
from pyathena import connect
import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Configuración de AWS
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_region = os.getenv('AWS_REGION')

# Configuración de Athena
ATHENA_OUTPUT_BUCKET = os.getenv('ATHENA_OUTPUT_BUCKET')
ATHENA_OUTPUT_PREFIX = os.getenv('ATHENA_OUTPUT_PREFIX')
ATHENA_DATABASE = os.getenv('ATHENA_DATABASE')


def ejecutar_query_athena(query):
    try:
        # Crear conexión a Athena
        conn = connect(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region,
            s3_staging_dir=f's3://{ATHENA_OUTPUT_BUCKET}/{ATHENA_OUTPUT_PREFIX}',
            schema_name=ATHENA_DATABASE
        )

        # Ejecutar query
        df = pd.read_sql(query, conn)
        print(f"Total de registros recuperados: {len(df)}")
        return df

    except Exception as e:
        print(f"Error executing Athena query: {e}")
        return None


def explore_cuadrantes():
    """
    Función exploratoria para entender la estructura y datos de la tabla cuadrantes
    """
    # 1. Primero ver la estructura básica
    query_basic = """
    SELECT *
    FROM cuandrantes
    LIMIT 5;
    """
    print("=== 1. Estructura básica ===")
    df_basic = ejecutar_query_athena(query_basic)
    print("\nColumnas disponibles:")
    print(df_basic.columns.tolist())
    print("\nTipos de datos:")
    print(df_basic.dtypes)

    # 2. Ver valores únicos de estacion
    query_stations = """
    SELECT DISTINCT estacion
    FROM cuandrantes
    ORDER BY estacion;
    """
    print("\n=== 2. Estaciones únicas ===")
    df_stations = ejecutar_query_athena(query_stations)
    print(df_stations)

    # 3. Verificar rangos de lat/long
    query_coords = """
    SELECT 
        MIN(latitud) as min_lat,
        MAX(latitud) as max_lat,
        MIN(longitud) as min_long,
        MAX(longitud) as max_long,
        COUNT(*) as total_points
    FROM cuandrantes;
    """
    print("\n=== 3. Rango de coordenadas ===")
    df_coords = ejecutar_query_athena(query_coords)
    print(df_coords)

    # 4. Ver cuántos puntos hay por cuadrante
    query_points = """
    SELECT 
        nro_cuadra,
        estacion,
        COUNT(*) as num_points
    FROM cuandrantes
    GROUP BY nro_cuadra, estacion
    ORDER BY num_points ASC
    LIMIT 10;
    """
    print("\n=== 4. Puntos por cuadrante (primeros 10) ===")
    df_points = ejecutar_query_athena(query_points)
    print(df_points)

    # 5. Ejemplo de un cuadrante específico
    query_single = """
    SELECT 
        nro_cuadra,
        estacion,
        latitud,
        longitud
    FROM cuandrantes
    WHERE nro_cuadra = (
        SELECT nro_cuadra 
        FROM cuandrantes 
        LIMIT 1
    );
    """
    print("\n=== 5. Ejemplo de un cuadrante ===")
    df_single = ejecutar_query_athena(query_single)
    print(df_single)

    return {
        "basic_structure": df_basic,
        "stations": df_stations,
        "coordinates_range": df_coords,
        "points_per_cuadrante": df_points,
        "single_cuadrante": df_single
    }


if __name__ == "__main__":
    # Ejecutar el análisis
    results = explore_cuadrantes()