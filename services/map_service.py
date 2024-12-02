import pandas as pd
import numpy as np
from typing import Dict, Any
from services.cuadrantes_service import CuadrantesService
from db.logger import setup_logger

logger = setup_logger("map_service")

class MapService:
    def __init__(self):
        self.cuadrantes_service = CuadrantesService()

    def prepare_map_data(self, data: Dict[str, Any], by_station: bool, data_type: str = "hurtos") -> Dict[str, Any]:
        try:
            gdf = self.cuadrantes_service.get_cuadrantes_base()
            if gdf is None:
                raise ValueError("No se pudieron obtener los cuadrantes base")

            gdf_merged = self._merge_data(gdf, data["data"], by_station)
            result = self._prepare_hover_data(gdf_merged, data, data_type)
            
            return self._create_geojson(result)
        except Exception as e:
            logger.error(f"Error en prepare_map_data: {e}")
            raise

    def _merge_data(self, gdf, data, by_station: bool):
        merged = gdf.merge(
            data,
            left_on='NRO_CUA',
            right_on='cuadrante_pol',
            how='left'
        )
        merged['total_cantidad'] = merged['total_cantidad'].fillna(0)
        
        if by_station:
            return merged.groupby('ESTACIO').apply(self._calculate_percentiles)
        return self._calculate_percentiles(merged)

    def _calculate_percentiles(self, group):
        group['percentile'] = pd.Series(group['total_cantidad']).rank(pct=True)
        max_percentile = group['percentile'].max()
        if pd.notnull(max_percentile) and max_percentile < 1:
            group['percentile'] = group['percentile'] / max_percentile
        return group

    def _prepare_hover_data(self, gdf, data, data_type):
        for cuadrante in pd.unique(data["hoverdata"]["cuadrante_pol"]):
            cuadrante_data = data["hoverdata"][
                data["hoverdata"]["cuadrante_pol"] == cuadrante
            ]
            total_casos = len(cuadrante_data)
            
            if total_casos > 0:
                stats = self._calculate_stats(cuadrante_data, total_casos, data_type)
                
                for key, value in stats.items():
                    gdf.loc[gdf["NRO_CUA"] == cuadrante, key] = value
                    
        return gdf

    def _calculate_stats(self, df, total_casos, data_type):
        stats = {}
        
        # Estadísticas comunes
        jornada_counts = df["jornada"].value_counts()
        stats.update({
            "porcentaje_manana": self._safe_percentage(jornada_counts.get("Mañana", 0), total_casos),
            "porcentaje_tarde": self._safe_percentage(jornada_counts.get("Tarde", 0), total_casos),
            "porcentaje_noche": self._safe_percentage(jornada_counts.get("Noche", 0), total_casos),
            "porcentaje_madrugada": self._safe_percentage(jornada_counts.get("Madrugada", 0), total_casos),
        })
        
        if data_type == "hurtos":
            genero_counts = df["sexo"].value_counts()
            stats.update({
                "porcentaje_masculino": self._safe_percentage(genero_counts.get("Hombre", 0), total_casos),
                "porcentaje_femenino": self._safe_percentage(genero_counts.get("Mujer", 0), total_casos),
                "porcentaje_sin_dato": self._safe_percentage(genero_counts.get("Sin dato", 0), total_casos),
                "modalidad": df["modalidad"].mode().iloc[0] if not df.empty else "",
                "sede_receptora": df["sede_receptora"].iloc[0] if not df.empty else ""
            })
            
        return stats

    def _safe_percentage(self, value: int, total: int) -> float:
        return np.round((value / total) * 100, 2) if total > 0 else 0

    def _create_geojson(self, gdf):
        features = []
        for _, row in gdf.iterrows():
            if pd.notnull(row['geometry']):
                feature = {
                    'type': 'Feature',
                    'geometry': row['geometry'].__geo_interface__,
                    'properties': {
                        col: float(row[col]) if isinstance(row[col], (int, float)) else row[col]
                        for col in gdf.columns
                        if col != 'geometry' and pd.notnull(row[col])
                    }
                }
                features.append(feature)
        
        return {
            'type': 'FeatureCollection',
            'features': features
        }