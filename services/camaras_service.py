from db.queries import DatabaseQueries

class CamarasService:
    def __init__(self):
        self.db_queries = DatabaseQueries()

    def get_camaras(self):
        """Obtiene ubicaciones de cámaras desde RDS PostgreSQL"""
        query = """
            SELECT 
                ST_X(ST_Transform(geom, 4326)) as longitude,
                ST_Y(ST_Transform(geom, 4326)) as latitude
            FROM shapes.camaras_2024
            WHERE ST_Y(ST_Transform(geom, 4326)) != 0;
        """
        try:
            result = self.db_queries.execute_rds_query(query)
            
            points = [
                {
                    'longitude': float(row['longitude']),
                    'latitude': float(row['latitude'])
                }
                for _, row in result.iterrows()
            ]

            return {
                'type': 'PointCollection',
                'points': points
            }
        except Exception as e:
            print(f"Error getting cameras data: {e}")
            raise Exception("Error obteniendo datos de cámaras desde RDS")