# routes/hurtos.py
from fastapi import APIRouter, HTTPException, Query, Depends
from datetime import date
from services.hurtos_service import HurtosService
from services.map_service import MapService

router = APIRouter(prefix="/hurtos", tags=["Hurtos"])


@router.get("/")
async def get_hurtos(
        start_date: date = Query(..., description="Fecha inicial"),
        end_date: date = Query(..., description="Fecha final"),
        tipo_hurto: str = Query("Todos", description="Tipo de hurto"),
        modalidad: str = Query("Todos", description="Modalidad del hurto"),
        by_station: bool = Query(False, description="Agrupar por estación"),
        hurtos_service: HurtosService = Depends(lambda: HurtosService()),
        map_service: MapService = Depends(lambda: MapService())
):
    try:
        # 1. Obtener datos de hurtos desde ATHENA
        hurtos_data = hurtos_service.get_hurtos_data(
            tipo_hurto, start_date, end_date, modalidad
        )

        # 2. Obtener shapes desde RDS y procesar
        map_data = map_service.prepare_map_data(hurtos_data, by_station,data_type="hurtos")

        return map_data

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

