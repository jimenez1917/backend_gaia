# routes/homicidios.py
from fastapi import APIRouter, HTTPException, Query, Depends
from datetime import date
from typing import List
from services.homicidios_service import HomicidiosService

router = APIRouter(prefix="/homicidios", tags=["Homicidios"])


@router.get("/")
async def get_homicidios(
        start_date: date = Query(..., description="Fecha inicial"),
        end_date: date = Query(..., description="Fecha final"),
        tipo_homicidio: str = Query("Todos", description="Tipo de homicidio"),
        modalidad: str = Query("Todos", description="Modalidad del homicidio"),
        homicidios_service: HomicidiosService = Depends(lambda: HomicidiosService())
):
    try:
        return homicidios_service.get_homicidios_data(
            tipo_homicidio, start_date, end_date, modalidad
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/modalidades/{tipo_homicidio}")
async def get_modalidades(
        tipo_homicidio: str,
        start_date: date = Query(..., description="Fecha inicial"),
        end_date: date = Query(..., description="Fecha final"),
        homicidios_service: HomicidiosService = Depends(lambda: HomicidiosService())
):
    """Obtiene las modalidades disponibles para un tipo de homicidio específico"""
    try:
        homicidio_class = homicidios_service.tipos_homicidio.get(tipo_homicidio)
        if not homicidio_class:
            raise HTTPException(status_code=400, detail="Tipo de homicidio no válido")

        # Actualiza los datos para el rango de fechas
        homicidio_class.update_data(start_date, end_date)
        return homicidio_class.get_modalidades()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/tipos", response_model=List[str])
async def get_tipos():
    """Obtiene los tipos de homicidios disponibles"""
    return ["Todos", "Feminicidio"]