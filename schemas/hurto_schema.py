from pydantic import BaseModel
from datetime import date
from typing import Dict, List, Any


class HurtoMetadata(BaseModel):
    tipo_hurto: str
    modalidad: str
    fecha_inicio: date
    fecha_fin: date
    total_registros: int

class HurtoResponse(BaseModel):
    data: Dict[str, Any]
    metadata: HurtoMetadata