# services/comisarias_service.py
from db.connections import DatabaseConnection
from schemas.comisarias import ComisariaLocationSchema, ComisariaCollectionSchema
from db.logger import setup_logger
import pandas as pd

logger = setup_logger("comisarias")

class ComisariasService:
   def __init__(self):
       self.db = DatabaseConnection().rds

   def get_comisarias(self) -> ComisariaCollectionSchema:
       query = """
           SELECT 
               ST_X(ST_Transform(geom, 4326)) as longitude,
               ST_Y(ST_Transform(geom, 4326)) as latitude
           FROM shapes."INSPECCIONES_DE_POLICIA"
           WHERE geom IS NOT NULL;
       """
       try:
           logger.info("Consultando ubicaciones de comisarías")
           result = self.db.execute_query(query)
           points = [
               ComisariaLocationSchema(
                   longitude=float(row['longitude']),
                   latitude=float(row['latitude'])
               )
               for _, row in result.iterrows()
               if not pd.isna(row['longitude']) and not pd.isna(row['latitude'])
           ]
           return ComisariaCollectionSchema(points=points)
       except Exception as e:
           logger.error(f"Error obteniendo datos de comisarías: {e}")
           raise