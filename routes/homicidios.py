from fastapi import APIRouter, HTTPException, Query, Depends
from datetime import date
from typing import List
from services.homicidios_service import HomicidiosService
from schemas.homicidios import HomicidioDataResponse

router = APIRouter(prefix="/homicidios", tags=["Homicidios"])

@router.get("/", response_model=HomicidioDataResponse)
async def get_homicidios(
       start_date: date = Query(..., description="Fecha inicial"),
       end_date: date = Query(..., description="Fecha final"),
       tipo_homicidio: str = Query("Todos", description="Tipo de homicidio"),
       modalidad: str = Query("Todos", description="Modalidad del homicidio"),
       homicidios_service: HomicidiosService = Depends(lambda: HomicidiosService())
) -> HomicidioDataResponse:
   try:
       return homicidios_service.get_homicidios_data(tipo_homicidio, start_date, end_date, modalidad)
   except Exception as e:
       raise HTTPException(status_code=500, detail=str(e))

@router.get("/modalidades/{tipo_homicidio}", response_model=List[str])
async def get_modalidades(
       tipo_homicidio: str,
       start_date: date = Query(..., description="Fecha inicial"),
       end_date: date = Query(..., description="Fecha final"),
       homicidios_service: HomicidiosService = Depends(lambda: HomicidiosService())
) -> List[str]:
   try:
       return homicidios_service.get_modalidades(tipo_homicidio, start_date, end_date)
   except Exception as e:
       raise HTTPException(status_code=500, detail=str(e))

@router.get("/tipos", response_model=List[str])
async def get_tipos() -> List[str]:
   return ["Todos", "Feminicidio"]