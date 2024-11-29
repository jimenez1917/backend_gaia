# models/homicidios.py
from abc import ABC, abstractmethod
from datetime import date
import pandas as pd
import numpy as np
import re
from fastapi import HTTPException
from pyproj import Transformer
from db.athena_rds_client import athena_query as ejecutar_query


class HOMICIDIOS(ABC):
    def __init__(self):
        self.data = None
        self.start_date = None
        self.end_date = None
        self.data_state = False

    def update_data(self, start_date: date, end_date: date, tipo: str = None):
        try:
            if not self.data_state:
                query = f"""SELECT 
                    barrio_hecho, caracterizacion, comuna_hecho, punto_plano_hecho, 
                    conducta, cuadrante_hecho, estacion_hecho, fecha_hecho, 
                    grupo_armado, grupo_caracterizacion, grupo_especial, grupo_feminicidio, 
                    homicidio_dbj, identidad_genero, lugar_hecho, modalidad, 
                    nombre_grupo_armado, orientacion_sexual
                FROM homicidio H 
                WHERE fecha_hecho >= CAST('{start_date} 00:00:00 UTC' AS TIMESTAMP) 
                AND fecha_hecho <= CAST('{end_date} 00:00:00 UTC' AS TIMESTAMP) 
                AND conducta = 'Homicidio' AND municipio_hecho = 'Medellín' 
                AND descartado = FALSE"""

                response = ejecutar_query(query, start_date, end_date)

                if isinstance(response, str):
                    raise HTTPException(status_code=500, detail=response)

                if response is None:
                    raise HTTPException(status_code=500, detail="No se recibió respuesta de Athena")

                if isinstance(response, pd.DataFrame):
                    result = response
                else:
                    result = pd.DataFrame(response["data"], columns=response["columns"])

                result["total_cantidad"] = 1
                result["fecha_hecho"] = pd.to_datetime(result['fecha_hecho'])
                self.start_date = start_date
                self.end_date = end_date
                self.data = result
                self.data_state = True

            return self.data

        except Exception as e:
            print(f"Error en update_data: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    def filter_modalidad(self, modalidad=None):
        try:
            if self.data is None or self.data.empty:
                return {
                    "hoverdata": [],
                    "data": []
                }

            if modalidad != "Todos":
                df = self.data[self.data['modalidad'] == modalidad].copy()
            else:
                df = self.data.copy()

            processed_df = self.prepare_density_map_data(df)

            return {
                "hoverdata": processed_df.to_dict('records'),  # Convertir a lista de diccionarios
                "data": processed_df.groupby('cuadrante_hecho')['total_cantidad']
                .sum()
                .reset_index()
                .to_dict('records')  # Convertir a lista de diccionarios
            }
        except Exception as e:
            print(f"Error en filter_modalidad: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    def prepare_density_map_data(self, df: pd.DataFrame) -> pd.DataFrame:
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

        # Convertir valores numpy a Python nativos
        for column in df.select_dtypes(include=[np.number]).columns:
            df[column] = df[column].astype(float)

        return df.drop(columns=['x', 'y'])

    def prepare_density_map_data(self, df: pd.DataFrame) -> pd.DataFrame:
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

    def get_modalidades(self):
        """Obtiene las modalidades desde los datos"""
        if self.data is None or self.data.empty:
            return ["Todos"]
        modalidades = self.data['modalidad'].unique()
        return ["Todos"] + list(modalidades)

class HomicidioTodos(HOMICIDIOS):
    def __init__(self):
        super().__init__()


class HomicidioFeminicidio(HOMICIDIOS):
    def filter_modalidad(self, modalidad=None):
        # Sobrescribimos para filtrar solo feminicidios
        if self.data is None or self.data.empty:
            return {"hoverdata": [], "data": []}

        # Primero filtramos por feminicidio
        df = self.data[self.data['grupo_feminicidio'] == 'Si'].copy()

        # Luego aplicamos el filtro de modalidad
        if modalidad and modalidad != "Todos":
            df = df[df['modalidad'] == modalidad]

        processed_df = self.prepare_density_map_data(df)

        return {
            "hoverdata": processed_df.to_dict('records'),
            "data": processed_df.groupby('cuadrante_hecho')['total_cantidad']
            .sum()
            .reset_index()
            .to_dict('records')
        }

    def get_modalidades(self):
        """Obtiene las modalidades solo para feminicidios"""
        if self.data is None or self.data.empty:
            return ["Todos"]
        df_feminicidios = self.data[self.data['grupo_feminicidio'] == 'Si']
        modalidades = df_feminicidios['modalidad'].unique()
        return ["Todos"] + list(modalidades)