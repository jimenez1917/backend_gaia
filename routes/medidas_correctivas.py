from fastapi import APIRouter, HTTPException, Query, Depends
from datetime import date
from typing import List, Dict, Any
from services.medidas_correctivas_service import MedidasCorrectivasService
from services.map_service import MapService
from schemas.medidas_correctivas import (
    MedidasCorrectivasResponse, 
    MedidasCorrectivasFilter
)

router = APIRouter(prefix="/medidas-correctivas", tags=["Medidas Correctivas"])

@router.get("/articulos", response_model=List[str])
async def get_articulos(
    medidas_service: MedidasCorrectivasService = Depends(lambda: MedidasCorrectivasService())
) -> List[str]:
    try:
        return medidas_service.get_articulos()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/articulo/numerales", response_model=List[str])
async def get_numerales(
    articulo: str = Query(..., description="Artículo específico"),
    medidas_service: MedidasCorrectivasService = Depends(lambda: MedidasCorrectivasService())
) -> List[str]:
    try:
        return medidas_service.get_comportamientos(articulo)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/", response_model=MedidasCorrectivasResponse)
async def get_medidas(
    filters: MedidasCorrectivasFilter = Depends(),
    by_station: bool = Query(False, description="Agrupar por estación"),
    medidas_service: MedidasCorrectivasService = Depends(lambda: MedidasCorrectivasService()),
    map_service: MapService = Depends(lambda: MapService())
) -> Dict[str, Any]:
    try:
        data = medidas_service.get_medidas_data(
            filters.start_date,
            filters.end_date,
            filters.articulo,
            filters.comportamiento
        )
        return map_service.prepare_map_data(data, by_station, "conductas")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))