# services/medidas_correctivas_service.py
from typing import Dict, Any, List
from datetime import date
from fastapi import HTTPException
from models.medidas_correctivas import MedidasCorrectivas


class MedidasCorrectivasService:
    def __init__(self):
        self.medidas = MedidasCorrectivas()

    def get_articulos(self) -> List[str]:
        """Obtiene la lista de artículos disponibles"""
        try:
            self.medidas.update_data(date(2024, 1, 1), date(2024, 12, 31))
            return self.medidas.get_articulos()
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    def get_comportamientos(self, articulo: str) -> List[str]:
        """Obtiene los comportamientos disponibles para un artículo"""
        try:
            self.medidas.update_data(date(2024, 1, 1), date(2024, 12, 31))
            return self.medidas.get_comportamientos(articulo)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    def get_medidas_data(
            self,
            start_date: date,
            end_date: date,
            articulo: str = None,
            comportamiento: str = None
    ) -> Dict[str, Any]:
        try:
            # Actualizar datos
            self.medidas.update_data(start_date, end_date)

            # Validar que el artículo y comportamiento sean válidos
            articulos_disponibles = self.medidas.get_articulos()
            if articulo and articulo not in articulos_disponibles:
                raise HTTPException(
                    status_code=400,
                    detail=f"Artículo no válido. Opciones: {articulos_disponibles}"
                )

            comportamientos_disponibles = self.medidas.get_comportamientos(articulo)
            if comportamiento and comportamiento not in comportamientos_disponibles:
                raise HTTPException(
                    status_code=400,
                    detail=f"Comportamiento no válido para {articulo}. Opciones: {comportamientos_disponibles}"
                )

            # Filtrar datos
            result = self.medidas.filter_data(articulo, comportamiento)

            return {
                "data": result["data"],
                "hoverdata": result["hoverdata"],
                "metadata": {
                    "articulo": articulo,
                    "comportamiento": comportamiento,
                    "fecha_inicio": str(start_date),
                    "fecha_fin": str(end_date),
                    "total_registros": len(result["hoverdata"])
                }
            }
        except Exception as e:
            print(f"Error en get_medidas_data: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))