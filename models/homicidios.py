from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Any

@dataclass
class Homicidio:
   barrio_hecho: str
   caracterizacion: str
   comuna_hecho: str
   cuadrante_hecho: str
   estacion_hecho: str
   fecha_hecho: datetime
   modalidad: str
   total_cantidad: int
   longitud: float
   latitud: float

   @classmethod
   def from_dataframe_row(cls, row: Dict[str, Any]):
       try:
           return cls(
               barrio_hecho=row['barrio_hecho'],
               caracterizacion=row['caracterizacion'],
               comuna_hecho=row['comuna_hecho'],
               cuadrante_hecho=row['cuadrante_hecho'],
               estacion_hecho=row['estacion_hecho'],
               fecha_hecho=row['fecha_hecho'],
               modalidad=row['modalidad'],
               total_cantidad=row['total_cantidad'],
               longitud=row['longitud'],
               latitud=row['latitud']
           )
       except KeyError as e:
           raise ValueError(f"Campo requerido faltante: {str(e)}")
       except Exception as e:
           raise ValueError(f"Error al crear Homicidio: {str(e)}")
