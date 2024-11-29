# models/percepcion.py
import pandas as pd
from typing import List, Dict, Any
import os
import re


class PercepcionModel:
    def __init__(self):
        self.df = None
        self.load_data()

    def parse_point(self, point_str: str) -> tuple:
        try:
            # Verificar si el punto está vacío o malformado
            if not point_str or point_str == 'POINT ( )':
                return None, None

            # Usar regex para extraer los números
            pattern = r'POINT\s*\(([-\d.]+)\s+([-\d.]+)\)'
            match = re.search(pattern, point_str)
            if match:
                lon = float(match.group(1))
                lat = float(match.group(2))
                return lon, lat
            else:
                print(f"No se pudo parsear el punto: {point_str}")
                return None, None
        except Exception as e:
            print(f"Error parseando el punto '{point_str}': {str(e)}")
            return None, None

    def load_data(self):
        try:
            # Construir la ruta relativa al archivo del modelo
            current_dir = os.path.dirname(os.path.abspath(__file__))
            file_path = os.path.join(current_dir, "data", "Capas encuesta.csv")

            # Leer el CSV
            self.df = pd.read_csv(file_path, sep='\t')

            # Extraer lat y lon usando el método más robusto
            coords = pd.DataFrame(
                self.df['Punto geográfico'].apply(self.parse_point).tolist(),
                columns=['lon', 'lat']
            )

            self.df['lon'] = coords['lon']
            self.df['lat'] = coords['lat']

            # Eliminar filas donde las coordenadas son None
            self.df = self.df.dropna(subset=['lon', 'lat'])

            # Convertir las columnas de preguntas a numéricas
            question_cols = [col for col in self.df.columns if col.startswith('P')]
            for col in question_cols:
                self.df[col] = pd.to_numeric(self.df[col], errors='coerce')

            print(f"Datos cargados exitosamente. Total de filas válidas: {len(self.df)}")

        except Exception as e:
            print(f"Error cargando los datos: {str(e)}")
            raise