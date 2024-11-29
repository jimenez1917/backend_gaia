import pandas as pd
#from utils.athena_rds_client_ssh import ejecutar_query_rds
from db.athena_rds_client import ejecutar_query_rds


class ComisariasService:
    def get_comisarias(self):
        """Obtiene solo las coordenadas de las comisarías para visualización en mapa"""
        try:
            query = """
                SELECT 
                    ST_X(ST_Transform(geom, 4326)) as longitude,
                    ST_Y(ST_Transform(geom, 4326)) as latitude
                FROM shapes."INSPECCIONES_DE_POLICIA"
                WHERE geom IS NOT NULL;
            """
            result = ejecutar_query_rds(query)

            if result is None or result.empty:
                return {'type': 'PointCollection', 'points': []}

            points = [
                {
                    'longitude': float(row['longitude']),
                    'latitude': float(row['latitude'])
                }
                for _, row in result.iterrows()
                if not pd.isna(row['longitude']) and not pd.isna(row['latitude'])
            ]

            return {
                'type': 'PointCollection',
                'points': points
            }

        except Exception as e:
            raise Exception(f"Error al obtener coordenadas: {str(e)}")