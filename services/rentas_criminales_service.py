from typing import Dict
import pandas as pd
import json
from fastapi import HTTPException
from db.connections import DatabaseConnection
from schemas.rentas_criminales import RentasCriminalesResponse
from models.rentas_criminales import RentaCriminal
from db.logger import setup_logger

logger = setup_logger("rentas_criminales")

class RentasCriminalesService:
   def __init__(self):
       self.db = DatabaseConnection().rds
       self.data = None
       self.data_state = False

   def _fetch_data(self) -> pd.DataFrame:
       if not self.data_state:
           query = """
               SELECT 
                   ST_AsGeoJSON(geom) as geom,
                   barrio,
                   comuna,
                   "pago por año (hogares)" as pago_anual
               FROM shapes.rentas_criminales
               WHERE barrio IS NOT NULL 
               AND comuna IS NOT NULL 
               AND "pago por año (hogares)" IS NOT NULL 
               AND "pago por año (hogares)" != '$-'
           """
           try:
               result = self.db.execute_query(query)
               result['pago_anual'] = result['pago_anual'].str.replace('$', '').str.replace('.', '').str.strip()
               result['pago_anual'] = pd.to_numeric(result['pago_anual'], errors='coerce')
               self.data = result.dropna()
               self.data_state = True
               return self.data
           except Exception as e:
               logger.error(f"Error en fetch_data: {str(e)}")
               raise

   def _calcular_percentil(self, data: pd.DataFrame, nivel: str) -> pd.Series:
       try:
           if nivel == 'ciudad':
               return data['pago_anual'].rank(pct=True) * 100
           elif nivel == 'comuna':
               return data.groupby('comuna')['pago_anual'].rank(pct=True) * 100
           raise ValueError(f"Nivel de agrupación no válido: {nivel}")
       except Exception as e:
           logger.error(f"Error calculando percentil: {str(e)}")
           raise

   def get_rentas_data(self, nivel_agrupacion: str = 'ciudad') -> Dict:
        try:
            data = self._fetch_data()
            data['percentil'] = self._calcular_percentil(data, nivel_agrupacion)

            features = [
                {
                    "type": "Feature",
                    "geometry": json.loads(row['geom']),
                    "properties": vars(RentaCriminal.from_row(row))
                }
                for _, row in data.iterrows()
            ]

            return RentasCriminalesResponse(
                type="FeatureCollection",
                features=features
            )
        except Exception as e:
            logger.error(f"Error en get_rentas_data: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))
