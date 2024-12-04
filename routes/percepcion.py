from fastapi import APIRouter, HTTPException, Depends, Query
from services.percepcion_service import PercepcionService
from schemas.percepcion import PercepcionResponse

router = APIRouter(prefix="/percepcion", tags=["Percepcion"])

@router.get("/pregunta/", response_model=PercepcionResponse)
async def get_percepcion(
   p: str = Query(..., description="Pregunta a consultar"),
   percepcion_service: PercepcionService = Depends(lambda: PercepcionService())
) -> PercepcionResponse:
   try:
       return percepcion_service.get_percepcion_data(p)
   except ValueError as e:
       raise HTTPException(status_code=400, detail=str(e))
   except Exception as e:
       raise HTTPException(status_code=500, detail=str(e))