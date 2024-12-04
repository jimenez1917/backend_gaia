from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Any, Optional

@dataclass
class MedidaCorrectiva:
    cuadrante_pol: str
    expediente: str
    municipio_hecho: str
    fecha_hecho: datetime
    cantidad: int
    jornada: str
    articulo: Optional[str]
    comportamiento: Optional[str]
    barrio_sisc: str
    comuna_sisc: str
    total_cantidad: int

    @classmethod
    def from_dataframe_row(cls, row: Dict[str, Any]) -> 'MedidaCorrectiva':
        try:
            return cls(
                cuadrante_pol=row['cuadrante_pol'],
                expediente=row['expediente'],
                municipio_hecho=row['municipio_hecho'],
                fecha_hecho=row['fecha_hecho'],
                cantidad=row['cantidad'],
                jornada=row['jornada'],
                articulo=row.get('articulo', ''),
                comportamiento=row.get('comportamiento', ''),
                barrio_sisc=row['barrio_sisc'],
                comuna_sisc=row['comuna_sisc'],
                total_cantidad=row['total_cantidad']
            )
        except KeyError as e:
            raise ValueError(f"Campo requerido faltante: {str(e)}")