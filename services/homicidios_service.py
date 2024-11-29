# services/homicidios_service.py
from typing import Dict, Any
from datetime import date
from fastapi import HTTPException
from models.homicidios import HomicidioTodos, HomicidioFeminicidio


class HomicidiosService:
    def __init__(self):
        self.tipos_homicidio = {
            'Todos': HomicidioTodos(),
            'Feminicidio': HomicidioFeminicidio()
        }

    def get_homicidios_data(
            self,
            tipo_homicidio: str,
            start_date: date,
            end_date: date,
            modalidad: str = "Todos"
    ) -> Dict[str, Any]:
        try:
            print(f"Procesando solicitud para tipo: {tipo_homicidio}, modalidad: {modalidad}")

            homicidio_class = self.tipos_homicidio.get(tipo_homicidio)
            if not homicidio_class:
                raise HTTPException(
                    status_code=400,
                    detail=f"Tipo de homicidio no válido"
                )

            # Primero actualizamos los datos
            homicidio_class.update_data(start_date, end_date)

            # Obtenemos las modalidades disponibles
            modalidades_disponibles = homicidio_class.get_modalidades()

            # Verificamos si la modalidad es válida
            if modalidad not in modalidades_disponibles:
                raise HTTPException(
                    status_code=400,
                    detail=f"Modalidad no válida para {tipo_homicidio}"
                )

            result = homicidio_class.filter_modalidad(modalidad)

            return {
                "data": result["data"],
                "hoverdata": result["hoverdata"],
                "metadata": {
                    "tipo_homicidio": tipo_homicidio,
                    "modalidad": modalidad,
                    "fecha_inicio": str(start_date),
                    "fecha_fin": str(end_date),
                    "total_registros": len(result["hoverdata"])
                }
            }

        except Exception as e:
            print(f"Error en get_homicidios_data: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))