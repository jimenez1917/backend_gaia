from shapely import wkb
import geopandas as gpd
#from utils.athena_rds_client_ssh import ejecutar_query_rds
from utils.athena_rds_client import ejecutar_query_rds


class CamarasService:
    def get_camaras(self):
        """Obtiene ubicaciones de cámaras desde RDS PostgreSQL"""
        query = """
            SELECT 
                ST_X(ST_Transform(geom, 4326)) as longitude,
                ST_Y(ST_Transform(geom, 4326)) as latitude
            FROM shapes.camaras_2024
            WHERE ST_Y(ST_Transform(geom, 4326)) != 0;
        """
        result = ejecutar_query_rds(query)

        if result is not None:
            # Convertir a formato de puntos para el mapa
            points = []
            for _, row in result.iterrows():
                point = {
                    'longitude': float(row['longitude']),
                    'latitude': float(row['latitude'])
                }
                points.append(point)

            return {
                'type': 'PointCollection',
                'points': points
            }
        else:
            raise Exception("Error obteniendo datos de cámaras desde RDS")