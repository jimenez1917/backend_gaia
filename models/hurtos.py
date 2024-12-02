from datetime import datetime
from typing import Any, Dict, List, Optional
from .base import BaseModel, BaseProperties

class HurtoBase(BaseModel):
    fecha_hecho: datetime
    cuadrante_pol: str
    sede_receptora: str
    jornada: str
    sexo: str
    modalidad: str
    conducta: Optional[str] = None

class HurtoProperties(BaseProperties):
    modalidad: str = ""
    sede_receptora: str = ""
    porcentaje_masculino: float = 0
    porcentaje_femenino: float = 0
    porcentaje_sin_dato: float = 0
    porcentaje_manana: float = 0
    porcentaje_tarde: float = 0
    porcentaje_noche: float = 0
    porcentaje_madrugada: float = 0

class HurtoFeature(BaseModel):
    type: str = "Feature"
    geometry: Dict[str, Any]
    properties: HurtoProperties

class HurtoCollection(BaseModel):
    type: str = "FeatureCollection"
    features: List[HurtoFeature]
