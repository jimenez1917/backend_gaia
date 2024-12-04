# services/camaras_service.py
from db.connections import DatabaseConnection
from schemas.camaras import CamaraLocationSchema, CamaraCollectionSchema
from db.logger import setup_logger

logger = setup_logger("camaras")

class CamarasService:
   def __init__(self):
       self.db = DatabaseConnection().rds

   def get_camaras(self) -> CamaraCollectionSchema:
       query = """
           SELECT 
               ST_X(ST_Transform(geom, 4326)) as longitude,
               ST_Y(ST_Transform(geom, 4326)) as latitude
           FROM shapes.camaras_2024
           WHERE ST_Y(ST_Transform(geom, 4326)) != 0;
       """
       try:
           logger.info("Consultando ubicaciones de cámaras")
           result = self.db.execute_query(query)
           points = [
               CamaraLocationSchema(
                   longitude=float(row['longitude']),
                   latitude=float(row['latitude'])
               )
               for _, row in result.iterrows()
           ]
           return CamaraCollectionSchema(points=points)
       except Exception as e:
           logger.error(f"Error obteniendo datos de cámaras: {e}")
           raise