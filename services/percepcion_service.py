from fastapi import HTTPException
import pandas as pd
import os
import re
from schemas.percepcion import PercepcionResponse, PercepcionSchema
from db.logger import setup_logger

logger = setup_logger("percepcion")

class PercepcionService:
   def __init__(self):
       self.df = None
       self.load_data()

   def parse_point(self, point_str: str) -> tuple:
       try:
           if not point_str or point_str == 'POINT ( )':
               return None, None
           pattern = r'POINT\s*\(([-\d.]+)\s+([-\d.]+)\)'
           match = re.search(pattern, point_str)
           if match:
               return float(match.group(1)), float(match.group(2))
           return None, None
       except Exception as e:
           logger.error(f"Error parseando punto: {str(e)}")
           return None, None

   def load_data(self):
       try:
           current_dir = os.path.dirname(os.path.abspath(__file__))
           file_path = os.path.join(current_dir, "..", "data", "Capas encuesta.csv")

           self.df = pd.read_csv(file_path, sep='\t')
           coords = pd.DataFrame(
               self.df['Punto geográfico'].apply(self.parse_point).tolist(),
               columns=['lon', 'lat']
           )
           self.df['lon'] = coords['lon']
           self.df['lat'] = coords['lat']
           self.df = self.df.dropna(subset=['lon', 'lat'])

           question_cols = [col for col in self.df.columns if col.startswith('P')]
           for col in question_cols:
               self.df[col] = pd.to_numeric(self.df[col], errors='coerce')

       except Exception as e:
           logger.error(f"Error cargando datos: {str(e)}")
           raise

   def _find_question_column(self, pregunta: str) -> str:
       patterns = [f'P{pregunta}.', f'P{pregunta} ', f'P{pregunta})', f'P{pregunta}:']
       for pattern in patterns:
           columns = [col for col in self.df.columns if col.startswith(pattern)]
           if columns:
               return columns[0]
       raise ValueError(f"Pregunta {pregunta} no encontrada")

   def get_percepcion_data(self, pregunta: str) -> PercepcionResponse:
       try:
           result = []
           if pregunta.lower() == 'todos':
               question_cols = [col for col in self.df.columns if col.startswith('P')]
               for _, row in self.df.iterrows():
                   values = [float(v) for v in row[question_cols] if v != 0 and not pd.isna(v)]
                   if values and not pd.isna(row['lat']) and not pd.isna(row['lon']):
                       result.append(PercepcionSchema(
                           latitude=float(row['lat']),
                           longitude=float(row['lon']),
                           valor=round(float(sum(values) / len(values)), 2)
                       ))
           else:
               column = self._find_question_column(pregunta)
               for _, row in self.df.iterrows():
                   if not pd.isna(row[column]) and not pd.isna(row['lat']) and not pd.isna(row['lon']):
                       result.append(PercepcionSchema(
                           latitude=float(row['lat']),
                           longitude=float(row['lon']),
                           valor=float(row[column])
                       ))
           return PercepcionResponse(data=result)
       except Exception as e:
           logger.error(f"Error: {str(e)}")
           raise HTTPException(status_code=500, detail=str(e))