from fastapi import APIRouter, HTTPException, Depends
from services.comisarias_service import ComisariasService

router = APIRouter(prefix="/comisarias", tags=["Comisarías e Inspecciones"])

@router.get("/")
async def get_comisarias(
    comisarias_service: ComisariasService = Depends(lambda: ComisariasService())
):
    """
    Obtiene la ubicación de todas las comisarías e inspecciones
    """
    try:
        return comisarias_service.get_comisarias()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))