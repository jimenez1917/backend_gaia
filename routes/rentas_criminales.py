from fastapi import APIRouter, HTTPException, Query, Depends
from services.rentas_criminales_service import RentasCriminalesService
from schemas.rentas_criminales import RentasCriminalesResponse

router = APIRouter(prefix="/rentas-criminales", tags=["Rentas Criminales"])

@router.get("/", response_model=RentasCriminalesResponse)
async def get_rentas(
   by_comuna: bool = Query(False, description="Calcular percentil por comuna"),
   rentas_service: RentasCriminalesService = Depends(lambda: RentasCriminalesService())
) -> RentasCriminalesResponse:
   try:
       nivel_agrupacion = 'comuna' if by_comuna else 'ciudad'
       return rentas_service.get_rentas_data(nivel_agrupacion)
   except Exception as e:
       raise HTTPException(status_code=500, detail=str(e))