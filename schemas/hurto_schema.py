# models/schemas/hurto_schema.py
from pydantic import BaseModel
from typing import Dict, List, Optional
from datetime import date

class HurtoBase(BaseModel):
    cuadrante_pol: str
    sede_receptora: str
    jornada: str
    sexo: str
    modalidad: str
    total_cantidad: int

class HurtoData(BaseModel):
    cuadrante_pol: str
    total_cantidad: int

class HurtoMetadata(BaseModel):
    tipo_hurto: str
    modalidad: str
    fecha_inicio: date
    fecha_fin: date
    total_registros: int

class HurtoResponse(BaseModel):
    hoverdata: List[HurtoBase]
    data: List[HurtoData]
    metadata: HurtoMetadata