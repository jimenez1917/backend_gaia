from pydantic import BaseModel
from typing import Dict, Any, List, Optional

class GeoJsonProperties(BaseModel):
    id: int
    nombre: str
    comuna: str
    type: str = "boundary"
    style: Dict[str, Any]

class GeoJsonGeometry(BaseModel):
    type: str
    coordinates: List[Any]

class GeoJsonFeature(BaseModel):
    type: str = "Feature"
    geometry: GeoJsonGeometry
    properties: GeoJsonProperties

class GeoJsonResponse(BaseModel):
    type: str = "FeatureCollection"
    features: List[GeoJsonFeature]
    metadata: Optional[Dict[str, Any]] = None

class CuadranteProperties(BaseModel):
    nro_cuadra: str
    estacion: str
    total_cantidad: float
    percentil: float
    modalidad_tendencia: Optional[str]
    porcentajes_jornada: Dict[str, float]
    porcentajes_genero: Dict[str, float]
    type: str = "cuadrante"
    style: Dict[str, Any]
