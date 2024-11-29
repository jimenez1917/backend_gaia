# services/percepcion_service.py
from typing import List, Dict, Any
from models.percepcion import PercepcionModel
import pandas as pd


class PercepcionService:
    def __init__(self, model: PercepcionModel):
        self.model = model

    def find_question_column(self, pregunta: str) -> str:
        """Encuentra la columna correcta para una pregunta dada"""
        df = self.model.df
        # Buscar de varias formas posibles
        patterns = [
            f'P{pregunta}.',
            f'P{pregunta} ',
            f'P{pregunta})',  # Por si hay paréntesis
            f'P{pregunta}:'  # Por si hay dos puntos
        ]

        for pattern in patterns:
            columns = [col for col in df.columns if col.startswith(pattern)]
            if columns:
                return columns[0]

        # Si no encuentra la columna, imprimir las columnas disponibles para debug
        print("Columnas disponibles:", df.columns.tolist())
        raise ValueError(f"Pregunta {pregunta} no encontrada")

    def get_percepcion_data(self, pregunta: str) -> List[Dict[str, float]]:
        df = self.model.df
        result = []

        try:
            if pregunta.lower() == 'todos':
                question_cols = [col for col in df.columns if col.startswith('P')]

                for _, row in df.iterrows():
                    values = [float(v) for v in row[question_cols]
                              if v != 0 and not pd.isna(v)]

                    if values:
                        avg_value = sum(values) / len(values)

                        if not pd.isna(row['lat']) and not pd.isna(row['lon']):
                            result.append({
                                "latitude": float(row['lat']),
                                "longitude": float(row['lon']),
                                "valor": round(float(avg_value), 2)
                            })
            else:
                try:
                    column = self.find_question_column(pregunta)

                    for _, row in df.iterrows():
                        if not pd.isna(row[column]) and not pd.isna(row['lat']) \
                                and not pd.isna(row['lon']):
                            result.append({
                                "latitude": float(row['lat']),
                                "longitude": float(row['lon']),
                                "valor": float(row[column])
                            })
                except ValueError as e:
                    print(f"Error buscando pregunta {pregunta}: {str(e)}")
                    raise

            return result

        except Exception as e:
            raise ValueError(str(e))