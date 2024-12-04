from typing import Dict, Any, List, Optional
from datetime import date
import pandas as pd
from fastapi import HTTPException
from db.connections import DatabaseConnection
from db.logger import setup_logger
from models.medidas_correctivas import MedidaCorrectiva

logger = setup_logger("medidas_correctivas")

class MedidasCorrectivasService:
    def __init__(self):
        self.db = DatabaseConnection()
        self.cache = {}
        self._init_cache()

    def _init_cache(self):
        """Inicializa el caché de artículos y comportamientos"""
        try:
            query = """
                SELECT DISTINCT articulo, comportamiento
                FROM aseco
                WHERE articulo IS NOT NULL 
                AND comportamiento IS NOT NULL
            """
            result = self.db.athena.execute_query(query)
            
            self.cache['articulos'] = ['Todos'] + sorted(result['articulo'].unique().tolist())
            self.cache['comportamientos'] = {
                articulo: ['Todos'] + sorted(comportamientos)
                for articulo, comportamientos in result.groupby('articulo')['comportamiento'].unique().items()
            }
            self.cache['comportamientos']['Todos'] = ['Todos']
        except Exception as e:
            logger.error(f"Error inicializando caché: {str(e)}")
            self.cache = {
                'articulos': ['Todos'],
                'comportamientos': {'Todos': ['Todos']}
            }

    def get_medidas_query(self, start_date: date, end_date: date, articulo: Optional[str] = None) -> str:
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')
        
        query = f"""
            SELECT 
                cuadrante_pol,
                expediente,
                municipio_hecho,
                fecha_hecho,
                '1' as cantidad,
                jornada,
                articulo,
                comportamiento,
                barrio_sisc,
                comuna_sisc
            FROM aseco
            WHERE CAST(fecha_hecho AS DATE) >= DATE '{start_str}'
            AND CAST(fecha_hecho AS DATE) <= DATE '{end_str}'
        """
        if articulo and articulo != "Todos":
            query += f" AND articulo = '{articulo}'"
        return query

    def validate_filters(self, articulo: Optional[str], comportamiento: Optional[str]) -> None:
        if articulo and articulo not in self.cache['articulos']:
            raise HTTPException(
                status_code=400,
                detail=f"Artículo no válido. Opciones: {self.cache['articulos']}"
            )
        
        if comportamiento and comportamiento != "Todos":
            valid_comportamientos = self.cache['comportamientos'].get(articulo or 'Todos', ['Todos'])
            if comportamiento not in valid_comportamientos:
                raise HTTPException(
                    status_code=400,
                    detail=f"Comportamiento no válido para {articulo}. Opciones: {valid_comportamientos}"
                )

    def get_articulos(self) -> List[str]:
        return self.cache['articulos']

    def get_comportamientos(self, articulo: str) -> List[str]:
        return self.cache['comportamientos'].get(articulo, ['Todos'])

    def get_medidas_data(
        self,
        start_date: date,
        end_date: date,
        articulo: Optional[str] = None,
        comportamiento: Optional[str] = None
    ) -> Dict[str, Any]:
        try:
            self.validate_filters(articulo, comportamiento)
            query = self.get_medidas_query(start_date, end_date, articulo)
            result = self.db.athena.execute_query(query)

            if result.empty:
                return self._empty_response(articulo, comportamiento, start_date, end_date)

            result["total_cantidad"] = 1
            filtered_data = self._filter_and_aggregate(result, comportamiento)

            return {
                "data": filtered_data["data"],
                "hoverdata": filtered_data["hoverdata"],
                "metadata": {
                    "articulo": articulo,
                    "comportamiento": comportamiento,
                    "fecha_inicio": start_date,
                    "fecha_fin": end_date,
                    "total_registros": len(filtered_data["hoverdata"])
                }
            }
        except Exception as e:
            logger.error(f"Error en get_medidas_data: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    def _empty_response(
        self,
        articulo: Optional[str],
        comportamiento: Optional[str],
        start_date: date,
        end_date: date
    ) -> Dict[str, Any]:
        return {
            "data": pd.DataFrame(),
            "hoverdata": pd.DataFrame(),
            "metadata": {
                "articulo": articulo,
                "comportamiento": comportamiento,
                "fecha_inicio": start_date,
                "fecha_fin": end_date,
                "total_registros": 0
            }
        }

    def _filter_and_aggregate(self, df: pd.DataFrame, comportamiento: Optional[str]) -> Dict[str, Any]:
        if comportamiento and comportamiento != "Todos":
            df = df[df["comportamiento"] == comportamiento]

        aggregated = df.groupby('cuadrante_pol')['total_cantidad'].sum().reset_index()

        return {
            "data": aggregated,
            "hoverdata": df
        }