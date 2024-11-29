# services/map_service.py
import geopandas as gpd
import pandas as pd
from utils.athena_rds_client import ejecutar_query_rds

####=====> Temporal

import os

class MapService:
    def get_cuadrantes_base(self):
        """Obtiene GeoDataFrame de cuadrantes desde RDS PostgreSQL"""
        query = """
            SELECT
                geom as geometry,
                nro_cuadra as "NRO_CUA",
                estacion as "ESTACIO"
            FROM shapes.cuadrantes
        """
        # Ahora llamamos a ejecutar_query_rds sin fechas
        result = ejecutar_query_rds(query)

        if result is not None:
            return gpd.GeoDataFrame(result)
        else:
            raise Exception("Error obteniendo datos de cuadrantes desde RDS")


    def prepare_map_data(self, hurtos_data: dict, by_station: bool):
        """Combina datos de hurtos de ATHENA con shapes de RDS"""
        # 1. Obtener shapes de cuadrantes desde RDS
        gdf = self.get_cuadrantes_base()

        # 2. Preparar datos básicos del mapa
        if by_station:
            gdf = gdf[['geometry', 'NRO_CUA', 'ESTACIO']]
            gdf_filtered = gdf.merge(
                hurtos_data["data"],
                left_on='NRO_CUA',
                right_on='cuadrante_pol',
                how='left'
            )
            gdf_cooked = gdf_filtered.groupby('ESTACIO').apply(self._calculate_percentiles).reset_index(drop=True)
        else:
            gdf = gdf[['geometry', 'NRO_CUA']]
            gdf_cooked = gdf.merge(
                hurtos_data["data"],
                left_on='NRO_CUA',
                right_on='cuadrante_pol',
                how='left'
            )
            gdf_cooked = self._calculate_percentiles(gdf_cooked)

        # 3. Agregar datos de hover
        print("gdf_cooked ======>",gdf_cooked)
        final_data = self._prepare_hover_data(gdf_cooked, hurtos_data)

        return final_data.to_json()

    def _calculate_percentiles(self, group):
        """Calcula los percentiles para colorear el mapa"""
        group['percentile'] = group['total_cantidad'].rank(pct=True)
        max_percentile = group['percentile'].max()
        if max_percentile < 1:
            group['percentile'] = group['percentile'] / max_percentile
        return group

    def _prepare_hover_data(self, gdf_filtered, df):
        """Prepara los datos de hover"""
        try:
            print("Iniciando _prepare_hover_data")
            print("Datos en df['hoverdata']:", df["hoverdata"].head())

            # 1. Cantidad Total
            cantidad = df["hoverdata"].groupby('cuadrante_pol')['total_cantidad'].first().reset_index()
            gdf_filtered["NRO_CUA"] = gdf_filtered["NRO_CUA"].astype(str).str.strip().str.upper()
            cantidad["cuadrante_pol"] = cantidad["cuadrante_pol"].astype(str).str.strip().str.upper()

            # Verificar coincidencias para cantidad
            matching_count = gdf_filtered["NRO_CUA"].isin(cantidad["cuadrante_pol"]).sum()
            print(f"Coincidencias en cantidad: {matching_count} de {len(gdf_filtered)}")

            result = gdf_filtered.merge(
                cantidad,
                left_on='NRO_CUA',
                right_on='cuadrante_pol',
                how='left',
                suffixes=('', '_cantidad')
            )
            print("Resultado después del merge de cantidad:", result[result['total_cantidad'].notna()].head())

            # 2. Estaciones
            estaciones = df["hoverdata"].groupby('cuadrante_pol')['sede_receptora'].first().reset_index()
            estaciones["cuadrante_pol"] = estaciones["cuadrante_pol"].astype(str).str.strip().str.upper()

            # Verificar coincidencias para estaciones
            matching_count = result["NRO_CUA"].isin(estaciones["cuadrante_pol"]).sum()
            print(f"Coincidencias en estaciones: {matching_count} de {len(result)}")

            result = result.merge(
                estaciones,
                left_on='NRO_CUA',
                right_on='cuadrante_pol',
                how='left',
                suffixes=('', '_estacion')
            )
            print("Resultado después del merge de estaciones:", result[result['sede_receptora'].notna()].head())

            # 3. Jornadas
            jornadas = df["hoverdata"].groupby('cuadrante_pol')['jornada'].value_counts(
                normalize=True).unstack().reset_index()
            jornadas = jornadas.fillna(0)
            jornadas["cuadrante_pol"] = jornadas["cuadrante_pol"].astype(str).str.strip().str.upper()

            # Renombrar columnas de jornada y verificar coincidencias
            jornadas = jornadas.rename(columns={
                'Mañana': 'porcentaje_manana',
                'Tarde': 'porcentaje_tarde',
                'Noche': 'porcentaje_noche',
                'Madrugada': 'porcentaje_madrugada'
            })
            matching_count = result["NRO_CUA"].isin(jornadas["cuadrante_pol"]).sum()
            print(f"Coincidencias en jornadas: {matching_count} de {len(result)}")

            result = result.merge(
                jornadas,
                left_on='NRO_CUA',
                right_on='cuadrante_pol',
                how='left',
                suffixes=('', '_jornada')
            )
            print("Resultado después del merge de jornadas:", result[result['porcentaje_noche'].notna()].head())

            # 4. Modalidad en Tendencia
            modalidades = df["hoverdata"].groupby('cuadrante_pol')['modalidad'].agg(
                lambda x: x.value_counts().index[0]).reset_index()
            modalidades["cuadrante_pol"] = modalidades["cuadrante_pol"].astype(str).str.strip().str.upper()

            # Verificar coincidencias para modalidad
            matching_count = result["NRO_CUA"].isin(modalidades["cuadrante_pol"]).sum()
            print(f"Coincidencias en modalidad: {matching_count} de {len(result)}")

            result = result.merge(
                modalidades,
                left_on='NRO_CUA',
                right_on='cuadrante_pol',
                how='left',
                suffixes=('', '_modalidad')
            )
            print("Resultado después del merge de modalidad:", result[result['modalidad'].notna()].head())
            # Porcentajes por género
            genero = df["hoverdata"].groupby('cuadrante_pol')['sexo'].value_counts(normalize=True).unstack().fillna(
                0).reset_index()

            # Aseguramos que las columnas 'Hombre', 'Mujer' y 'Sin dato' existen en el DataFrame
            for col in ['Hombre', 'Mujer', 'Sin dato']:
                if col not in genero.columns:
                    genero[col] = 0  # Agregar la columna con 0 si no existe

            # Calcular los porcentajes redondeados
            genero['porcentaje_masculino'] = (genero['Hombre'] * 100).round(2)
            genero['porcentaje_femenino'] = (genero['Mujer'] * 100).round(2)
            genero['porcentaje_sin_dato'] = (genero['Sin dato'] * 100).round(2)

            # Realizamos el merge asegurando que solo incluimos las columnas necesarias
            result = result.merge(
                genero[['cuadrante_pol', 'porcentaje_masculino', 'porcentaje_femenino', 'porcentaje_sin_dato']],
                left_on='NRO_CUA',
                right_on='cuadrante_pol',
                how='left',
                suffixes=('', '_genero')
            )
            print("Resultado después del merge de género:", result[result['porcentaje_masculino'].notna()].head())
            return result

        except Exception as e:
            print(f"Error en prepare_hover_data: {str(e)}")
            raise

    # def _prepare_hover_data(self, gdf_filtered, df):
    #     """Prepara los datos de hover"""
    #     try:
    #         # Cantidad total
    #         cantidad = df["hoverdata"].groupby('cuadrante_pol')['total_cantidad'].first().reset_index()
    #         result = gdf_filtered.merge(
    #             cantidad,
    #             left_on='NRO_CUA',
    #             right_on='cuadrante_pol',
    #             how='left'
    #         )
    #         # Eliminamos la columna cuadrante_pol duplicada después de cada merge
    #         if 'cuadrante_pol' in result.columns:
    #             result = result.drop('cuadrante_pol', axis=1)
    #
    #         # Estaciones
    #         estaciones = df["hoverdata"].groupby('cuadrante_pol')['sede_receptora'].first().reset_index()
    #         result = result.merge(
    #             estaciones,
    #             left_on='NRO_CUA',
    #             right_on='cuadrante_pol',
    #             how='left'
    #         )
    #         if 'cuadrante_pol' in result.columns:
    #             result = result.drop('cuadrante_pol', axis=1)
    #
    #         # Porcentajes por jornada
    #         jornadas = df["hoverdata"].groupby('cuadrante_pol')['jornada'].value_counts(
    #             normalize=True).unstack().reset_index()
    #         jornadas = jornadas.fillna(0)
    #         for col in ['Mañana', 'Tarde', 'Noche', 'Madrugada']:
    #             if col not in jornadas.columns:
    #                 jornadas[col] = 0
    #         jornadas = jornadas.rename(columns={
    #             'Mañana': 'porcentaje_manana',
    #             'Tarde': 'porcentaje_tarde',
    #             'Noche': 'porcentaje_noche',
    #             'Madrugada': 'porcentaje_madrugada'
    #         })
    #         for col in ['porcentaje_manana', 'porcentaje_tarde', 'porcentaje_noche', 'porcentaje_madrugada']:
    #             jornadas[col] = (jornadas[col] * 100).round(2)
    #
    #         result = result.merge(
    #             jornadas,
    #             left_on='NRO_CUA',
    #             right_on='cuadrante_pol',
    #             how='left'
    #         )
    #         if 'cuadrante_pol' in result.columns:
    #             result = result.drop('cuadrante_pol', axis=1)
    #
    #         # Modalidad en tendencia
    #         modalidades = df["hoverdata"].groupby('cuadrante_pol')['modalidad'].agg(
    #             lambda x: x.value_counts().index[0] if len(x) > 0 else None
    #         ).reset_index()
    #         result = result.merge(
    #             modalidades,
    #             left_on='NRO_CUA',
    #             right_on='cuadrante_pol',
    #             how='left'
    #         )
    #         if 'cuadrante_pol' in result.columns:
    #             result = result.drop('cuadrante_pol', axis=1)
    #
    #         # Porcentajes por género
    #         genero = df["hoverdata"].groupby('cuadrante_pol')['sexo'].value_counts(
    #             normalize=True).unstack().reset_index()
    #         genero = genero.fillna(0)
    #         if 'Hombre' in genero.columns:
    #             genero['porcentaje_masculino'] = (genero['Hombre'] * 100).round(2)
    #         if 'Mujer' in genero.columns:
    #             genero['porcentaje_femenino'] = (genero['Mujer'] * 100).round(2)
    #         if 'Sin dato' in genero.columns:
    #             genero['porcentaje_sin_dato'] = (genero['Sin dato'] * 100).round(2)
    #
    #         result = result.merge(
    #             genero[[col for col in
    #                     ['cuadrante_pol', 'porcentaje_masculino', 'porcentaje_femenino', 'porcentaje_sin_dato']
    #                     if col in genero.columns]],
    #             left_on='NRO_CUA',
    #             right_on='cuadrante_pol',
    #             how='left'
    #         )
    #         if 'cuadrante_pol' in result.columns:
    #             result = result.drop('cuadrante_pol', axis=1)
    #
    #         return result
    #
    #     except Exception as e:
    #         print(f"Error en prepare_hover_data: {str(e)}")
    #         raise

    # def get_cuadrantes_base(self):
    #     """Obtiene GeoDataFrame de cuadrantes desde RDS PostgreSQL"""
    #      query = """
    #          SELECT
    #              geom as geometry,
    #              nro_cuadra as "NRO_CUA",
    #              estacion as "ESTACIO"
    #          FROM shapes.cuadrantes
    #      """
    #     # Ahora llamamos a ejecutar_query_rds sin fechas
    #     result = ejecutar_query_rds(query)
    #     # script_dir = os.path.dirname(os.path.abspath(__file__))
    #     # print(script_dir)
    #     # def load_and_process_shapefile(filepath, target_crs="EPSG:4326"):
    #     #     gdf = gpd.read_file(filepath)
    #     #     gdf = gdf.to_crs(target_crs)
    #     #     return gdf
    #     # result = load_and_process_shapefile(os.path.join(script_dir, 'data', 'Cuadrantes.shp'))
    #     if result is not None:
    #         return gpd.GeoDataFrame(result)
    #     else:
    #         raise Exception("Error obteniendo datos de cuadrantes desde RDS")