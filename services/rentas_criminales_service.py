from models.rentas_criminales import RentasCriminales
import pandas as pd
from fastapi import HTTPException
import json

class RentasCriminalesService:
    def __init__(self):
        self.model = RentasCriminales()

    def get_rentas_data(self, nivel_agrupacion='ciudad'):
        try:
            data = self.model.update_data()
            data['pago_anual'] = pd.to_numeric(data['pago_anual'], errors='coerce')
            data = data.dropna(subset=['pago_anual'])
            data['percentil'] = self._calcular_percentil(data, nivel_agrupacion)

            features = []
            for _, row in data.iterrows():
                feature = {
                    "type": "Feature",
                    "geometry": json.loads(row['geom']),
                    "properties": {
                        "barrio": row['barrio'],
                        "comuna": row['comuna'],
                        "pago_anual": float(row['pago_anual']),
                        "percentil": float(row['percentil'])
                    }
                }
                features.append(feature)

            return {
                "type": "FeatureCollection",
                "features": features
            }

        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    def _calcular_percentil(self, data, nivel):
        try:
            if nivel == 'ciudad':
                return data['pago_anual'].rank(pct=True) * 100
            elif nivel == 'comuna':
                return data.groupby('comuna')['pago_anual'].rank(pct=True) * 100
            else:
                raise ValueError(f"Nivel de agrupación no válido: {nivel}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error calculando percentil: {str(e)}")