# routes/percepcion.py
from fastapi import APIRouter, HTTPException
from typing import List, Dict
from services.percepcion_service import PercepcionService
from models.percepcion import PercepcionModel

router = APIRouter(
    prefix="/percepcion",
    tags=["Percepcion"]
)

percepcion_model = PercepcionModel()
percepcion_service = PercepcionService(percepcion_model)

@router.get("/pregunta/", response_model=List[Dict[str, float]])
async def get_percepcion(p: str):
    try:
        return percepcion_service.get_percepcion_data(p)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Error interno del servidor")
