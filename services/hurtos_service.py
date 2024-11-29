# services/hurtos_service.py
from typing import Dict, Any
from datetime import date
from fastapi import HTTPException
from models.hurtos import (
    HurtoTodos, HurtoPersona, HurtoCarro, HurtoMoto,
    HurtoEstablecimientoComercial, HurtoResidencia
)


class HurtosService:
    def __init__(self):
        self.tipos_hurto = {
            'Todos': HurtoTodos(),
            'Hurto a persona': HurtoPersona(),
            'Hurto de carro': HurtoCarro(),
            'Hurto de moto': HurtoMoto(),
            'Hurto a establecimiento comercial': HurtoEstablecimientoComercial(),
            'Hurto a residencia': HurtoResidencia()
        }

    def get_hurtos_data(
            self,
            tipo_hurto: str,
            start_date: date,
            end_date: date,
            modalidad: str = "Todos"
    ) -> Dict[str, Any]:
        try:
            print(f"Procesando solicitud para tipo: {tipo_hurto}, modalidad: {modalidad}")

            if tipo_hurto not in self.tipos_hurto:
                raise HTTPException(
                    status_code=400,
                    detail=f"Tipo de hurto no válido. Opciones: {list(self.tipos_hurto.keys())}"
                )

            hurto_class = self.tipos_hurto[tipo_hurto]
            if modalidad not in hurto_class.desagregaciones:
                raise HTTPException(
                    status_code=400,
                    detail=f"Modalidad no válida para {tipo_hurto}. Opciones: {hurto_class.desagregaciones}"
                )

            hurto_class.update_data(start_date, end_date, tipo_hurto if tipo_hurto != "Todos" else None)
            result = hurto_class.filter_modalidad(modalidad)
            print("acaaaa_1",result["hoverdata"].head())
            print("acaaa_2",result["hoverdata"][result["hoverdata"]["cuadrante_pol"] == "MEVALMNVCCD05E01C01000010"].head())
            return {
                "data": result["data"],
                "hoverdata": result["hoverdata"],
                "metadata": {
                    "tipo_hurto": tipo_hurto,
                    "modalidad": modalidad,
                    "fecha_inicio": start_date,
                    "fecha_fin": end_date,
                    "total_registros": len(result["hoverdata"])
                }
            }

        except Exception as e:
            print(f"Error en get_hurtos_data: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))