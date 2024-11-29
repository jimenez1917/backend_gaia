# models/hurtos.py
from abc import ABC, abstractmethod
from datetime import date
import pandas as pd
from fastapi import HTTPException
from db.athena_rds_client import athena_query as ejecutar_query


class HURTOS(ABC):
    def __init__(self, desagregados):
        self.data = None
        self.start_date = None
        self.end_date = None
        self.desagregaciones = desagregados
        self.data_state = False

    def update_data(self, start_date, end_date, conducta=None):
        try:
            print("Iniciando update_data con:", conducta)
            if not self.data_state:
                if conducta == None:
                    query = """
                        SELECT fecha_hecho, conducta, cuadrante_pol, sede_receptora, jornada, sexo, modalidad
                        FROM hurtos
                        WHERE fecha_hecho >= CAST('{start_date} 00:00:00 UTC' AS TIMESTAMP)
                            AND fecha_hecho <= CAST('{end_date} 00:00:00 UTC' AS TIMESTAMP)
                    """
                else:
                    query = """
                        SELECT fecha_hecho, cuadrante_pol, sede_receptora, jornada, sexo, modalidad
                        FROM hurtos
                        WHERE conducta = '{}'
                            AND fecha_hecho >= CAST('{{start_date}} 00:00:00 UTC' AS TIMESTAMP)
                            AND fecha_hecho <= CAST('{{end_date}} 00:00:00 UTC' AS TIMESTAMP)
                    """.format(conducta)

                print("Query a ejecutar:", query)
                response = ejecutar_query(query, start_date, end_date)
                print("Respuesta de Athena:", response)

                if isinstance(response, str):
                    raise HTTPException(status_code=500, detail=response)

                if response is None:
                    raise HTTPException(status_code=500, detail="No se recibió respuesta de Athena")

                # Si la respuesta ya es un DataFrame, la usamos directamente
                if isinstance(response, pd.DataFrame):
                    result = response
                else:
                    # Si no, la convertimos a DataFrame
                    result = pd.DataFrame(response["data"], columns=response["columns"])

                print("DataFrame creado:", result.head())

                result["total_cantidad"] = 1
                result["fecha_hecho"] = pd.to_datetime(result['fecha_hecho'])
                self.start_date = start_date
                self.end_date = end_date
                self.data = result
                self.data_state = True

            print("Data actual:", self.data.head() if self.data is not None else None)
            return self.data

        except Exception as e:
            print(f"Error en update_data: {str(e)}")
            print(f"Tipo de respuesta:", type(response))  # Debug adicional
            print(f"Contenido de respuesta:", response)  # Debug adicional
            raise HTTPException(status_code=500, detail=str(e))
    def filter_modalidad(self, modalidad=None):
        try:
            if modalidad != None and modalidad != "Todos":
                df = self.data.query(f"modalidad == '{modalidad}'").copy()
            else:
                df = self.data.copy()

            # Convertimos el DataFrame en el formato que espera prepare_map_data
            return {
                "hoverdata": df.copy(),  # Datos completos para el hover
                "data": df.groupby('cuadrante_pol')['total_cantidad'].sum().reset_index()
                # Datos agregados por cuadrante
            }
        except Exception as e:
            print(f"Error en filter_modalidad: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))
# Las clases específicas heredan de HURTOS
class HurtoTodos(HURTOS):
    def __init__(self):
        super().__init__(['Todos', 'Atraco', 'Descuido', 'Halado', 'Cosquilleo', 'Raponazo'])

class HurtoPersona(HURTOS):
    def __init__(self):
        super().__init__(['Todos', 'Atraco', 'Descuido', 'Cosquilleo', 'Raponazo', 'Engaño'])

class HurtoCarro(HURTOS):
    def __init__(self):
        super().__init__(['Todos', 'Halado', 'Atraco', 'Descuido', 'Engaño', 'Escopolamina'])

class HurtoMoto(HURTOS):
    def __init__(self):
        super().__init__(['Todos', 'Halado', 'Descuido', 'Engaño', 'Escopolamina'])

class HurtoEstablecimientoComercial(HURTOS):
    def __init__(self):
        super().__init__(['Todos', 'Descuido', 'Atraco', 'Rompimiento cerradura', 'Mechero', 'Engaño'])

class HurtoResidencia(HURTOS):
    def __init__(self):
        super().__init__(['Todos', 'Descuido', 'Rompimiento cerradura', 'Atraco', 'Engaño', 'Llave maestra'])