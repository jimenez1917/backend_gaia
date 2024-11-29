#from shapely import wkb
from shapely.geometry import Polygon
import geopandas as gpd
import pandas as pd
import numpy as np
from db.athena_rds_client import ejecutar_query_rds
#from utils.athena_rds_client_ssh import ejecutar_query_rds

class MapService:
    def get_cuadrantes_base(self):
        """Obtiene GeoDataFrame de cuadrantes desde Athena construyendo geometrías desde coordenadas"""
        query = """
            SELECT
                nro_cuadra as "NRO_CUA",
                estacion as "ESTACIO",
                array_agg(longitud) as longitudes,
                array_agg(latitud) as latitudes,
                count(*) as num_points
            FROM cuandrantes 
            WHERE estacion IN (
                'POPULAR', 'SANTA CRUZ', 'MANRIQUE', 'ARANJUEZ', 
                'DOCE DE OCTUBRE', 'CASTILLA', 'VILLA HERMOSA', 
                'BUENOS AIRES', 'SAN JAVIER', 'LAURELES', 
                'SAN ANTONIO DE PRADO', 'BELEN', 'LA CANDELARIA', 'POBLADO'
            )
            GROUP BY nro_cuadra, estacion
            HAVING count(*) >= 3  -- Aseguramos tener al menos 3 puntos diferentes
        """
        result = ejecutar_query_athena(query)

        if result is not None:
            def create_polygon(row):
                try:
                    # Convertir las strings de arrays a listas de coordenadas
                    longitudes = eval(row['longitudes'])  # Convertir string de array a lista
                    latitudes = eval(row['latitudes'])  # Convertir string de array a lista

                    # Crear lista de coordenadas [lon, lat]
                    coords = [[float(lon), float(lat)] for lon, lat in zip(longitudes, latitudes)]

                    # Eliminar duplicados manteniendo el orden
                    unique_coords = []
                    seen = set()
                    for coord in coords:
                        coord_tuple = tuple(coord)
                        if coord_tuple not in seen:
                            seen.add(coord_tuple)
                            unique_coords.append(coord)

                    # Verificar si tenemos suficientes puntos únicos
                    if len(unique_coords) < 3:
                        print(f"Cuadrante {row['NRO_CUA']} no tiene suficientes puntos únicos")
                        return None

                    # Asegurar que el polígono está cerrado
                    if unique_coords[0] != unique_coords[-1]:
                        unique_coords.append(unique_coords[0])

                    # Intentar crear el polígono
                    poly = Polygon(unique_coords)
                    if not poly.is_valid:
                        print(f"Polígono inválido para cuadrante {row['NRO_CUA']}")
                        return None

                    return poly

                except Exception as e:
                    print(f"Error creando polígono para cuadrante {row['NRO_CUA']}: {str(e)}")
                    return None

            # Crear geometrías desde las coordenadas
            result['geometry'] = result.apply(create_polygon, axis=1)

            # Eliminar filas donde no se pudo crear el polígono
            result = result[result['geometry'].notna()]

            # Eliminar las columnas de coordenadas y num_points
            result = result.drop(['longitudes', 'latitudes', 'num_points'], axis=1)

            # Crear GeoDataFrame solo con polígonos válidos
            gdf = gpd.GeoDataFrame(result, geometry='geometry')

            print(f"Total de cuadrantes procesados exitosamente: {len(gdf)}")

            return gdf
        else:
            raise Exception("Error obteniendo datos de cuadrantes desde Athena")

    # def get_cuadrantes_base(self):
    #     """Obtiene GeoDataFrame de cuadrantes desde RDS PostgreSQL"""
    #     query = """
    #         SELECT
    #             geom as geometry,
    #             nro_cuadra as "NRO_CUA",
    #             estacion as "ESTACIO"
    #         FROM shapes.cuadrantes
    #         WHERE estacion IN (
    #             'POPULAR',
    #             'SANTA CRUZ',
    #             'MANRIQUE',
    #             'ARANJUEZ',
    #             'DOCE DE OCTUBRE',
    #             'CASTILLA',
    #             'VILLA HERMOSA',
    #             'BUENOS AIRES',
    #             'SAN JAVIER',
    #             'LAURELES',
    #             'SAN ANTONIO DE PRADO',
    #             'BELEN',
    #             'LA CANDELARIA',
    #             'POBLADO'
    #         );
    #     """
    #     result = ejecutar_query_rds(query)
    #
    #     if result is not None:
    #         # Convertir la columna geometry de WKB a objetos shapely
    #         result['geometry'] = result['geometry'].apply(lambda x: wkb.loads(bytes.fromhex(x)))
    #
    #         # Crear GeoDataFrame con la geometría convertida
    #         gdf = gpd.GeoDataFrame(result, geometry='geometry')
    #
    #         return gdf
    #     else:
    #         raise Exception("Error obteniendo datos de cuadrantes desde RDS")

    def _calculate_percentiles(self, group):
        """Calcula los percentiles para colorear el mapa"""
        if 'total_cantidad' not in group.columns:
            group['total_cantidad'] = 0
        group['percentile'] = pd.Series(group['total_cantidad']).rank(pct=True)
        max_percentile = group['percentile'].max()
        if pd.notnull(max_percentile) and max_percentile < 1:
            group['percentile'] = group['percentile'] / max_percentile
        return group

    def _calculate_common_stats(self, cuadrante_data: pd.DataFrame, total_casos: int) -> dict:
        """Calcula estadísticas comunes para ambos tipos de datos"""
        stats = {"total_cantidad": total_casos}

        # Porcentajes por jornada (común para ambos tipos)
        jornada_counts = cuadrante_data["jornada"].value_counts()
        stats.update({
            "porcentaje_manana": np.round((jornada_counts.get("Mañana", 0) / total_casos) * 100,
                                          2) if total_casos > 0 else 0,
            "porcentaje_tarde": np.round((jornada_counts.get("Tarde", 0) / total_casos) * 100,
                                         2) if total_casos > 0 else 0,
            "porcentaje_noche": np.round((jornada_counts.get("Noche", 0) / total_casos) * 100,
                                         2) if total_casos > 0 else 0,
            "porcentaje_madrugada": np.round((jornada_counts.get("Madrugada", 0) / total_casos) * 100,
                                             2) if total_casos > 0 else 0
        })

        return stats

    def _calculate_hurtos_stats(self, cuadrante_data: pd.DataFrame, total_casos: int) -> dict:
        """Calcula estadísticas específicas para hurtos"""
        stats = {}

        # Porcentajes por género
        genero_counts = cuadrante_data["sexo"].value_counts()
        stats.update({
            "porcentaje_masculino": np.round((genero_counts.get("Hombre", 0) / total_casos) * 100,
                                             2) if total_casos > 0 else 0,
            "porcentaje_femenino": np.round((genero_counts.get("Mujer", 0) / total_casos) * 100,
                                            2) if total_casos > 0 else 0,
            "porcentaje_sin_dato": np.round((genero_counts.get("Sin dato", 0) / total_casos) * 100,
                                            2) if total_casos > 0 else 0
        })

        # Modalidad y sede receptora
        stats.update({
            "modalidad": cuadrante_data["modalidad"].mode().iloc[0] if not cuadrante_data.empty else "",
            "sede_receptora": cuadrante_data["sede_receptora"].iloc[0] if not cuadrante_data.empty else ""
        })

        return stats

    def _calculate_conductas_stats(self, cuadrante_data: pd.DataFrame) -> dict:
        """Calcula estadísticas específicas para conductas"""
        stats = {
            "articulo": cuadrante_data["articulo"].mode().iloc[0] if not cuadrante_data.empty else "",
            "comportamiento": cuadrante_data["comportamiento"].mode().iloc[0] if not cuadrante_data.empty else ""
        }

        return stats

    def _prepare_hover_data(self, gdf_filtered: gpd.GeoDataFrame, df: dict, data_type: str) -> gpd.GeoDataFrame:
        """Prepara los datos de hover según el tipo de dato"""
        try:
            # Trabajamos con los datos agrupados por cuadrante
            cuadrante_stats = {}

            # Iteramos sobre cada cuadrante único
            for cuadrante in df["hoverdata"]["cuadrante_pol"].unique():
                # Filtramos datos para este cuadrante
                cuadrante_data = df["hoverdata"][df["hoverdata"]["cuadrante_pol"] == cuadrante]
                total_casos = cuadrante_data['total_cantidad'].sum()

                # Calcular estadísticas comunes
                stats = self._calculate_common_stats(cuadrante_data, total_casos)

                # Agregar estadísticas específicas según el tipo
                if data_type == "hurtos":
                    stats.update(self._calculate_hurtos_stats(cuadrante_data, total_casos))
                else:  # conductas
                    stats.update(self._calculate_conductas_stats(cuadrante_data))

                cuadrante_stats[cuadrante] = stats

            # Combinamos las estadísticas con el GeoDataFrame
            result = gdf_filtered.copy()

            # Agregamos todas las columnas disponibles en las estadísticas
            all_columns = set().union(*[set(stats.keys()) for stats in cuadrante_stats.values()])
            for col in all_columns:
                result[col] = result["NRO_CUA"].map(lambda x: cuadrante_stats.get(x, {}).get(col, 0))

            return result

        except Exception as e:
            print(f"Error en _prepare_hover_data: {str(e)}")
            raise

    def prepare_map_data(self, data: dict, by_station: bool, data_type: str = "hurtos"):
        """Prepara los datos del mapa según el tipo de dato"""
        try:
            # 1. Obtener shapes de cuadrantes
            gdf = self.get_cuadrantes_base()

            # 2. Preparar datos básicos del mapa
            if by_station:
                gdf = gdf[['geometry', 'NRO_CUA', 'ESTACIO']]
                gdf_filtered = gdf.merge(
                    data["data"],
                    left_on='NRO_CUA',
                    right_on='cuadrante_pol',
                    how='left'
                )
                gdf_cooked = gdf_filtered.groupby('ESTACIO').apply(self._calculate_percentiles).reset_index(drop=True)
            else:
                gdf = gdf[['geometry', 'NRO_CUA']]
                gdf_filtered = gdf.merge(
                    data["data"],
                    left_on='NRO_CUA',
                    right_on='cuadrante_pol',
                    how='left'
                )
                gdf_cooked = self._calculate_percentiles(gdf_filtered)

            # 3. Preparar los datos de hover
            hover_data = self._prepare_hover_data(gdf_cooked.copy(), data, data_type)

            # 4. Estructurar la respuesta final
            result = {
                'type': 'FeatureCollection',
                'features': []
            }

            for _, row in hover_data.iterrows():
                if pd.notnull(row['geometry']):
                    geometry = (row['geometry'].to_wkt() if hasattr(row['geometry'], 'to_wkt')
                                else row['geometry'].__geo_interface__ if hasattr(row['geometry'], '__geo_interface__')
                    else str(row['geometry']))
                else:
                    geometry = None

                # Propiedades base
                properties = {
                    'NRO_CUA': str(row['NRO_CUA']),
                    'total_cantidad': float(row['total_cantidad']) if pd.notnull(row['total_cantidad']) else 0,
                    'percentile': float(row['percentile']) if pd.notnull(row['percentile']) else 0,
                }

                # Agregar propiedades según el tipo de dato
                if data_type == "hurtos":
                    properties.update({
                        'modalidad': str(row['modalidad']) if pd.notnull(row['modalidad']) else '',
                        'sede_receptora': str(row['sede_receptora']) if pd.notnull(row['sede_receptora']) else '',
                        'porcentaje_masculino': float(row['porcentaje_masculino']) if pd.notnull(
                            row['porcentaje_masculino']) else 0,
                        'porcentaje_femenino': float(row['porcentaje_femenino']) if pd.notnull(
                            row['porcentaje_femenino']) else 0,
                        'porcentaje_sin_dato': float(row['porcentaje_sin_dato']) if pd.notnull(
                            row['porcentaje_sin_dato']) else 0,
                    })
                else:  # conductas
                    properties.update({
                        'articulo': str(row['articulo']) if pd.notnull(row['articulo']) else '',
                        'comportamiento': str(row['comportamiento']) if pd.notnull(row['comportamiento']) else '',
                    })

                # Agregar propiedades comunes (jornada)
                properties.update({
                    'porcentaje_manana': float(row['porcentaje_manana']) if pd.notnull(row['porcentaje_manana']) else 0,
                    'porcentaje_tarde': float(row['porcentaje_tarde']) if pd.notnull(row['porcentaje_tarde']) else 0,
                    'porcentaje_noche': float(row['porcentaje_noche']) if pd.notnull(row['porcentaje_noche']) else 0,
                    'porcentaje_madrugada': float(row['porcentaje_madrugada']) if pd.notnull(
                        row['porcentaje_madrugada']) else 0,
                })

                feature = {
                    'type': 'Feature',
                    'geometry': geometry,
                    'properties': properties
                }
                result['features'].append(feature)

            return result

        except Exception as e:
            print(f"Error en prepare_map_data: {str(e)}")
            raise