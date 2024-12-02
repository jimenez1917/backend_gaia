# services/hurtos_service.py
from typing import Dict, Any, Optional
from datetime import date
import pandas as pd
from fastapi import HTTPException
from db.connections import DatabaseConnection
from db.logger import setup_logger


logger = setup_logger("hurtos")

class HurtosService:
    def __init__(self):
        self.db = DatabaseConnection()
        self._init_tipos()

    def _init_tipos(self):
        self.tipos = {
            'Todos': {
                'desagregaciones': ['Todos', 'Atraco', 'Descuido', 'Halado', 'Cosquilleo', 'Raponazo']
            },
            'Hurto a persona': {
                'desagregaciones': ['Todos', 'Atraco', 'Descuido', 'Cosquilleo', 'Raponazo', 'Engaño']
            },
            'Hurto de carro': {
                'desagregaciones': ['Todos', 'Halado', 'Atraco', 'Descuido', 'Engaño', 'Escopolamina']
            },
            'Hurto de moto': {
                'desagregaciones': ['Todos', 'Halado', 'Descuido', 'Engaño', 'Escopolamina']
            },
            'Hurto a establecimiento comercial': {
                'desagregaciones': ['Todos', 'Descuido', 'Atraco', 'Rompimiento cerradura', 'Mechero', 'Engaño']
            },
            'Hurto a residencia': {
                'desagregaciones': ['Todos', 'Descuido', 'Rompimiento cerradura', 'Atraco', 'Engaño', 'Llave maestra']
            }
        }

    def validate_tipo_hurto(self, tipo_hurto: str, modalidad: str) -> None:
        if tipo_hurto not in self.tipos:
            raise HTTPException(
                status_code=400, 
                detail=f"Tipo de hurto no válido. Opciones: {list(self.tipos.keys())}"
            )
        
        if modalidad not in self.tipos[tipo_hurto]['desagregaciones']:
            raise HTTPException(
                status_code=400,
                detail=f"Modalidad no válida para {tipo_hurto}. Opciones: {self.tipos[tipo_hurto]['desagregaciones']}"
            )

    # services/hurtos_service.py
    def get_hurtos_query(self, start_date: date, end_date: date, tipo_hurto: str = None) -> str:
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')
        
        query = f"""
            SELECT 
                fecha_hecho,
                cuadrante_pol,
                sede_receptora,
                jornada,
                sexo,
                modalidad
            FROM hurtos
            WHERE fecha_hecho >= CAST('{start_str} 00:00:00 UTC' AS TIMESTAMP)
                AND fecha_hecho <= CAST('{end_str} 00:00:00 UTC' AS TIMESTAMP)
        """
        if tipo_hurto and tipo_hurto != "Todos":
            query += f" AND conducta = '{tipo_hurto}'"
        return query

    def get_hurtos_data(self, tipo_hurto: str, start_date: date, end_date: date, 
                       modalidad: str = "Todos") -> Dict[str, Any]:
        try:
            self.validate_tipo_hurto(tipo_hurto, modalidad)
            query = self.get_hurtos_query(start_date, end_date, tipo_hurto)
            result = self.db.athena.execute_query(query)

            if result.empty:
                return self._empty_response(tipo_hurto, modalidad, start_date, end_date)

            result["total_cantidad"] = 1
            filtered_data = self._filter_and_aggregate(result, modalidad)

            return {
                "data": filtered_data["data"],
                "hoverdata": filtered_data["hoverdata"],
                "metadata": {
                    "tipo_hurto": tipo_hurto,
                    "modalidad": modalidad,
                    "fecha_inicio": start_date,
                    "fecha_fin": end_date,
                    "total_registros": len(filtered_data["hoverdata"])
                }
            }
        except Exception as e:
            logger.error(f"Error en get_hurtos_data: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    def _empty_response(self, tipo_hurto: str, modalidad: str, 
                       start_date: date, end_date: date) -> Dict[str, Any]:
        return {
            "data": pd.DataFrame(),
            "hoverdata": pd.DataFrame(),
            "metadata": {
                "tipo_hurto": tipo_hurto,
                "modalidad": modalidad,
                "fecha_inicio": start_date,
                "fecha_fin": end_date,
                "total_registros": 0
            }
        }

    def _filter_and_aggregate(self, df: pd.DataFrame, modalidad: str) -> Dict[str, Any]:
        if modalidad != "Todos":
            df = df[df["modalidad"] == modalidad]

        aggregated = df.groupby('cuadrante_pol')['total_cantidad'].sum().reset_index()

        return {
            "data": aggregated,
            "hoverdata": df
        }

    def calculate_percentages(self, df: pd.DataFrame) -> Dict[str, float]:
        total = len(df)
        if total == 0:
            return {
                "porcentaje_masculino": 0,
                "porcentaje_femenino": 0,
                "porcentaje_sin_dato": 0,
                "porcentaje_manana": 0,
                "porcentaje_tarde": 0,
                "porcentaje_noche": 0,
                "porcentaje_madrugada": 0
            }

        genero_counts = df["sexo"].value_counts()
        jornada_counts = df["jornada"].value_counts()

        return {
            "porcentaje_masculino": (genero_counts.get("Hombre", 0) / total) * 100,
            "porcentaje_femenino": (genero_counts.get("Mujer", 0) / total) * 100,
            "porcentaje_sin_dato": (genero_counts.get("Sin dato", 0) / total) * 100,
            "porcentaje_manana": (jornada_counts.get("Mañana", 0) / total) * 100,
            "porcentaje_tarde": (jornada_counts.get("Tarde", 0) / total) * 100,
            "porcentaje_noche": (jornada_counts.get("Noche", 0) / total) * 100,
            "porcentaje_madrugada": (jornada_counts.get("Madrugada", 0) / total) * 100
        }