from typing import List
import pandas as pd
import numpy as np
import re
from datetime import date
from pyproj import Transformer
from fastapi import HTTPException
from schemas.homicidios import HomicidioDataResponse
from db.logger import setup_logger
from db.connections import DatabaseConnection
from models.homicidios import Homicidio

logger = setup_logger("homicidios")

class HomicidiosService:
   def __init__(self):
       self.db = DatabaseConnection().athena
       self.data = None
       self.data_state = False
       self.start_date = None
       self.end_date = None

   def _fetch_data(self, start_date: date, end_date: date) -> pd.DataFrame:
       if not self.data_state or self.start_date != start_date or self.end_date != end_date:
           query = f"""
               SELECT 
                   barrio_hecho, caracterizacion, comuna_hecho, punto_plano_hecho, 
                   conducta, cuadrante_hecho, estacion_hecho, fecha_hecho, 
                   grupo_armado, grupo_caracterizacion, grupo_especial, grupo_feminicidio, 
                   homicidio_dbj, identidad_genero, lugar_hecho, modalidad, 
                   nombre_grupo_armado, orientacion_sexual
               FROM homicidio H 
               WHERE fecha_hecho >= CAST('{start_date} 00:00:00 UTC' AS TIMESTAMP) 
               AND fecha_hecho <= CAST('{end_date} 00:00:00 UTC' AS TIMESTAMP) 
               AND conducta = 'Homicidio' AND municipio_hecho = 'Medellín' 
               AND descartado = FALSE
           """
           try:
               result = self.db.execute_query(query)
               result["total_cantidad"] = 1
               result["fecha_hecho"] = pd.to_datetime(result['fecha_hecho'])
               self.data = result
               self.data_state = True
               self.start_date = start_date
               self.end_date = end_date
           except Exception as e:
               logger.error(f"Error en fetch_data: {str(e)}")
               raise
       return self.data

   def _prepare_density_map_data(self, df: pd.DataFrame) -> pd.DataFrame:
       def extract_coordinates(point_str):
           match = re.search(r'POINT \((\d+\.\d+) (\d+\.\d+)\)', point_str)
           if match:
               return float(match.group(1)), float(match.group(2))
           return np.nan, np.nan

       def convert_coordinates(x, y):
           transformer = Transformer.from_crs("EPSG:3116", "EPSG:4326", always_xy=True)
           lon, lat = transformer.transform(x, y)
           return lon, lat

       df = df.copy()
       df[['x', 'y']] = df['punto_plano_hecho'].apply(lambda x: pd.Series(extract_coordinates(x)))
       df[['longitud', 'latitud']] = df.apply(
           lambda row: pd.Series(convert_coordinates(row['x'], row['y'])), axis=1)
       return df.drop(columns=['x', 'y'])

   def get_homicidios_data(self, tipo_homicidio: str, start_date: date, end_date: date, modalidad: str = "Todos") -> HomicidioDataResponse:
       try:
           df = self._fetch_data(start_date, end_date)
           if tipo_homicidio == "Feminicidio":
               df = df[df['grupo_feminicidio'] == 'Si']
           if modalidad != "Todos":
               df = df[df['modalidad'] == modalidad]

           processed_df = self._prepare_density_map_data(df)
           
           try:
               homicidios = [Homicidio.from_dataframe_row(row) for _, row in processed_df.iterrows()]
           except ValueError as e:
               logger.error(f"Error al convertir datos: {str(e)}")
               raise HTTPException(status_code=500, detail="Error al procesar los datos de homicidios")

           return HomicidioDataResponse(
               data=processed_df.groupby('cuadrante_hecho')['total_cantidad'].sum().reset_index().to_dict('records'),
               hoverdata=[vars(h) for h in homicidios],
               metadata={
                   "tipo_homicidio": tipo_homicidio,
                   "modalidad": modalidad,
                   "fecha_inicio": str(start_date),
                   "fecha_fin": str(end_date),
                   "total_registros": len(processed_df)
               }
           )
       except Exception as e:
           logger.error(f"Error en get_homicidios_data: {str(e)}")
           raise HTTPException(status_code=500, detail=str(e))
   def get_modalidades(self, tipo_homicidio: str, start_date: date, end_date: date) -> List[str]:
       df = self._fetch_data(start_date, end_date)
       if tipo_homicidio == "Feminicidio":
           df = df[df['grupo_feminicidio'] == 'Si']
       modalidades = df['modalidad'].unique()
       return ["Todos"] + list(modalidades)