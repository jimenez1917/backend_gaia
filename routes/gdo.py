from fastapi import APIRouter, HTTPException, Depends
from services.gdo_service import GDOService

router = APIRouter(prefix="/gdo", tags=["GDO-GDCO"])

@router.get("/")
async def get_gdo(
    gdo_service: GDOService = Depends(lambda: GDOService())
):
    """
    Obtiene la ubicación de todos los puntos GDO-GDCO
    """
    try:
        return gdo_service.get_gdo()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))