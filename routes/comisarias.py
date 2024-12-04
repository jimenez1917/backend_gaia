from fastapi import APIRouter, HTTPException, Depends
from services.comisarias_service import ComisariasService
from schemas.comisarias import ComisariaCollectionSchema

router = APIRouter(prefix="/comisarias", tags=["Comisarías e Inspecciones"])

@router.get("/", response_model=ComisariaCollectionSchema)
async def get_comisarias(
   comisarias_service: ComisariasService = Depends(lambda: ComisariasService())
) -> ComisariaCollectionSchema:
   try:
       return comisarias_service.get_comisarias()
   except Exception as e:
       raise HTTPException(status_code=500, detail=str(e))