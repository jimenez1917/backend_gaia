from fastapi import APIRouter, HTTPException, Depends
from services.geo_data_service import GeoDataService

router = APIRouter(prefix="/geo_base", tags=["Geometrías Base"])

@router.get("/")
async def get_geo_base():
    try:
        geo_service = GeoDataService()
        # Usar geometrías simplificadas para mejor rendimiento
        return geo_service.get_simplified_geometries(tolerance=0.0001)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))