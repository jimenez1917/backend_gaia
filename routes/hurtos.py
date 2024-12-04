from fastapi import APIRouter, HTTPException, Query, Depends
from datetime import date
from services.hurtos_service import HurtosService
from services.map_service import MapService
from models.hurtos import HurtoCollection
from db.logger import setup_logger

logger = setup_logger("hurtos")  # __name__ dará el nombre del módulo actual

router = APIRouter(prefix="/hurtos", tags=["Hurtos"])

@router.get("/", response_model=HurtoCollection)
async def get_hurtos(
    start_date: date = Query(..., description="Fecha inicial"),
    end_date: date = Query(..., description="Fecha final"),
    tipo_hurto: str = Query("Todos", description="Tipo de hurto"),
    modalidad: str = Query("Todos", description="Modalidad del hurto"),
    by_station: bool = Query(False, description="Agrupar por estación"),
    hurtos_service: HurtosService = Depends(),
    map_service: MapService = Depends()
):
    try:
        hurtos_data = hurtos_service.get_hurtos_data(
            tipo_hurto=tipo_hurto,
            start_date=start_date,
            end_date=end_date,
            modalidad=modalidad
        )
        
        map_data = map_service.prepare_map_data(
            data=hurtos_data,
            by_station=by_station,
            data_type="hurtos"
        )
        
        return map_data
    except Exception as e:
        logger.error(f"Error en endpoint get_hurtos: {e}")
        raise HTTPException(status_code=500, detail=str(e))