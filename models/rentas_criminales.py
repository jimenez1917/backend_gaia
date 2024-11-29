#from utils.athena_rds_client_ssh import ejecutar_query_rds
from utils.athena_rds_client import ejecutar_query_rds
import pandas as pd
from fastapi import HTTPException


class RentasCriminales:
    def __init__(self):
        self.data = None
        self.data_state = False

    def update_data(self):
        try:
            if not self.data_state:
                query = """
                    SELECT 
                        ST_AsGeoJSON(geom) as geom,
                        barrio,
                        comuna,
                        "pago por año (hogares)" as pago_anual
                    FROM shapes.rentas_criminales
                    WHERE barrio IS NOT NULL 
                    AND comuna IS NOT NULL 
                    AND "pago por año (hogares)" IS NOT NULL 
                    AND "pago por año (hogares)" != '$-'
                """

                result = ejecutar_query_rds(query)
                print("Resultado consulta inicial:", result.shape)

                if result is None or isinstance(result, pd.DataFrame) and result.empty:
                    raise HTTPException(
                        status_code=500,
                        detail="No se encontraron datos"
                    )

                # Limpieza de pago_anual
                result['pago_anual'] = result['pago_anual'].str.replace('$', '').str.replace('.', '').str.strip()
                result['pago_anual'] = pd.to_numeric(result['pago_anual'], errors='coerce')

                # Eliminar filas con valores nulos después de la conversión
                result = result.dropna()

                print("Datos después de limpieza:", result.shape)
                print("Muestra de datos limpios:", result.head())

                self.data = result
                self.data_state = True

            return self.data

        except Exception as e:
            print(f"Error en update_data: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Error al procesar los datos: {str(e)}"
            )