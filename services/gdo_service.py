#from utils.athena_rds_client_ssh import ejecutar_query_rds
from utils.athena_rds_client import ejecutar_query_rds

class GDOService:
    def get_gdo(self):
        """Obtiene ubicaciones de GDO-GDCO desde RDS PostgreSQL"""
        query = """
            SELECT 
                ST_X(ST_Transform(geom, 4326)) as longitude,
                ST_Y(ST_Transform(geom, 4326)) as latitude
            FROM shapes."GDO-GDCO";
        """
        result = ejecutar_query_rds(query)

        if result is not None:
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
            raise Exception("Error obteniendo datos de GDO-GDCO desde RDS")