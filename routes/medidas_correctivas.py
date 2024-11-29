# routes/medidas_correctivas.py
from fastapi import APIRouter, HTTPException, Query, Depends
from datetime import date
from services.medidas_correctivas_service import MedidasCorrectivasService
from services.map_service import MapService

router = APIRouter(prefix="/medidas-correctivas", tags=["Medidas Correctivas"])


@router.get("/articulos")
async def get_articulos(
        medidas_service: MedidasCorrectivasService = Depends(lambda: MedidasCorrectivasService())
):
    """Obtiene los artículos disponibles"""
    try:
        return medidas_service.get_articulos()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/articulo/numerales")
async def get_numerales(
        articulo: str = Query(..., description="Artículo específico"),
        medidas_service: MedidasCorrectivasService = Depends(lambda: MedidasCorrectivasService())
):
    """Obtiene los numerales disponibles para un artículo específico"""
    try:
        return medidas_service.get_comportamientos(articulo)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/")
async def get_medidas(
        start_date: date = Query(..., description="Fecha inicial"),
        end_date: date = Query(..., description="Fecha final"),
        articulo: str = Query(None, description="Filtro por artículo"),
        comportamiento: str = Query(None, description="Filtro por comportamiento"),
        by_station: bool = Query(False, description="Agrupar por estación"),
        medidas_service: MedidasCorrectivasService = Depends(lambda: MedidasCorrectivasService()),
        map_service: MapService = Depends(lambda: MapService())
):
    """Obtiene las medidas correctivas según los filtros especificados"""
    try:
        # Obtener datos de medidas correctivas
        medidas_data = medidas_service.get_medidas_data(
            start_date,
            end_date,
            articulo,
            comportamiento
        )
        # Preparar datos para el mapa
        map_data = map_service.prepare_map_data(medidas_data, by_station,data_type="conductas")

        return map_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
