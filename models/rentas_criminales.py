# models/rentas_criminales.py
from dataclasses import dataclass
from typing import Dict

@dataclass
class RentaCriminal:
   barrio: str
   comuna: str
   pago_anual: float
   percentil: float

   @classmethod
   def from_row(cls, row: Dict):
       return cls(
           barrio=row['barrio'],
           comuna=row['comuna'],
           pago_anual=float(row['pago_anual']),
           percentil=float(row['percentil'])
       )