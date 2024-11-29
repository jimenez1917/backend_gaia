from datetime import date
import pandas as pd
from fastapi import HTTPException
from utils.athena_rds_client import athena_query

class MedidasCorrectivas:
    def __init__(self):
        self.data = None
        self.start_date = None
        self.end_date = None
        self.data_state = False
        self.articulos_comportamientos = {}

    def update_data(self, start_date: date, end_date: date, articulo=None):
        try:
            if not self.data_state:
                if articulo == None:
                    query = """
                        SELECT 
                            cuadrante_pol,
                            expediente,
                            municipio_hecho,
                            fecha_hecho,
                            '1' as cantidad,
                            jornada,
                            articulo,
                            comportamiento,
                            barrio_sisc,
                            comuna_sisc
                        FROM aseco
                        WHERE CAST(fecha_hecho AS DATE) >= DATE '{start_date}'
                        AND CAST(fecha_hecho AS DATE) <= DATE '{end_date}'
                    """
                else:
                    query = f"""
                        SELECT 
                            cuadrante_pol,
                            expediente,
                            municipio_hecho,
                            fecha_hecho,
                            '1' as cantidad,
                            jornada,
                            comportamiento,
                            barrio_sisc,
                            comuna_sisc
                        FROM aseco
                        WHERE articulo = '{articulo}'
                        AND CAST(fecha_hecho AS DATE) >= DATE '{start_date}'
                        AND CAST(fecha_hecho AS DATE) <= DATE '{end_date}'
                    """
                formatted_query = query.format(start_date=start_date, end_date=end_date)
                print("Query final:", formatted_query)
                result = athena_query(formatted_query, start_date, end_date)


                if result is None or isinstance(result, pd.DataFrame) and result.empty:
                    raise HTTPException(
                        status_code=500,
                        detail="No se encontraron datos para el período especificado"
                    )

                # Convertir result a DataFrame si no lo es
                if not isinstance(result, pd.DataFrame):
                    result = pd.DataFrame(result)

                # Asegurarse de que las columnas existan
                required_columns = ['cantidad', 'fecha_hecho', 'cuadrante_pol']
                missing_columns = [col for col in required_columns if col not in result.columns]
                if missing_columns:
                    raise HTTPException(
                        status_code=500,
                        detail=f"Faltan columnas requeridas en la respuesta: {', '.join(missing_columns)}"
                    )

                result["total_cantidad"] = pd.to_numeric(result['cantidad'].fillna(1))
                result["fecha_hecho"] = pd.to_datetime(result['fecha_hecho'])
                self.start_date = start_date
                self.end_date = end_date
                self.data = result
                self.data_state = True

                if articulo is None:
                    self._update_articulos_comportamientos()

            return self.data

        except HTTPException as he:
            print(f"HTTP Exception en update_data: {str(he)}")
            raise he
        except Exception as e:
            print(f"Error en update_data: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Error al procesar los datos: {str(e)}"
            )

    # El resto de los métodos permanecen igual
    def _update_articulos_comportamientos(self):
        if self.data is not None and not self.data.empty:
            grupos = self.data.groupby('articulo')['comportamiento'].unique()
            self.articulos_comportamientos = {
                articulo: ['Todos'] + list(comportamientos)
                for articulo, comportamientos in grupos.items()
            }
            self.articulos_comportamientos['Todos'] = ['Todos']

    def get_articulos(self):
        if self.data is None or self.data.empty:
            return ['Todos']
        return ['Todos'] + sorted(self.data['articulo'].unique().tolist())

    def get_comportamientos(self, articulo: str = None):
        if articulo == 'Todos' or articulo is None:
            return ['Todos']
        return self.articulos_comportamientos.get(articulo, ['Todos'])

    def filter_data(self, articulo: str = None, comportamiento: str = None):
        try:
            df = self.data.copy()

            if articulo and articulo != "Todos":
                df = df[df['articulo'] == articulo]
            if comportamiento and comportamiento != "Todos":
                df = df[df['comportamiento'] == comportamiento]

            return {
                "hoverdata": df.copy(),
                "data": df.groupby('cuadrante_pol')['total_cantidad'].sum().reset_index()
            }
        except Exception as e:
            print(f"Error en filter_data: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))