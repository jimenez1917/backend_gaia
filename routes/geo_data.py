# routes/geo_routes.py
from fastapi import APIRouter, HTTPException, Depends
from services.geo_data_service import GeoDataService
from schemas.geo_schema import GeoBaseResponseSchema

router = APIRouter(prefix="/geo_base", tags=["Geometrías Base"])

@router.get("/", response_model=GeoBaseResponseSchema)
async def get_geo_base():
   try:
       geo_service = GeoDataService()
       return geo_service.get_simplified_geometries(tolerance=0.0001)
   except Exception as e:
       raise HTTPException(status_code=500, detail=str(e))