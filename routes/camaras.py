from fastapi import APIRouter, HTTPException, Depends
from services.camaras_service import CamarasService
from schemas.camaras import CamaraCollectionSchema

router = APIRouter(prefix="/camaras", tags=["Cámaras"])

@router.get("/", response_model=CamaraCollectionSchema)
async def get_camaras(
   camaras_service: CamarasService = Depends(lambda: CamarasService())
) -> CamaraCollectionSchema:
   try:
       return camaras_service.get_camaras()
   except Exception as e:
       raise HTTPException(status_code=500, detail=str(e))