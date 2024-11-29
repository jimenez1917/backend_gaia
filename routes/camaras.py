from fastapi import APIRouter, HTTPException, Depends
from services.camaras_service import CamarasService

router = APIRouter(prefix="/camaras", tags=["Cámaras"])

@router.get("/")
async def get_camaras(
    camaras_service: CamarasService = Depends(lambda: CamarasService())
):
    """
    Obtiene la ubicación de todas las cámaras de seguridad
    """
    try:
        return camaras_service.get_camaras()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))