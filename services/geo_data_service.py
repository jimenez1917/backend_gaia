# services/geo_data_service.py
from shapely import wkb
import geopandas as gpd
from typing import Dict, Optional, Any
from dataclasses import dataclass
from db.connections import DatabaseConnection
from db.logger import setup_logger
from models.geo import FeatureCollectionModel, FeatureModel

logger = setup_logger("geo_service")

@dataclass
class GeoJSONFeatureCollection:
    type: str = "FeatureCollection"
    features: list = None

    def __post_init__(self):
        if self.features is None:
            self.features = []

    def add_feature(self, geometry: Any, properties: Dict[str, Any]) -> None:
        feature = {
            "type": "Feature",
            "geometry": geometry,
            "properties": properties
        }
        self.features.append(feature)

    def to_dict(self) -> Dict:
        return {
            "type": self.type,
            "features": self.features
        }

class GeoDataService:
    def __init__(self):
        self.db = DatabaseConnection()
        self.logger = setup_logger(self.__class__.__name__)

    def _execute_geo_query(self, query: str, entity_name: str) -> Optional[gpd.GeoDataFrame]:
        try:
            result = self.db.rds.execute_query(query)
            if result.empty:
                raise ValueError(f"No se obtuvieron datos para {entity_name}")
            result['geometry'] = result['geometry'].apply(
                lambda x: wkb.loads(bytes.fromhex(x))
            )
            return gpd.GeoDataFrame(result, geometry='geometry')
        except Exception as e:
            self.logger.error(f"Error al obtener {entity_name}: {str(e)}")
            raise

    def get_comunas_corregimientos(self) -> gpd.GeoDataFrame:
        query = """
            SELECT 
                geom as geometry,
                nombre
            FROM shapes.limite_catastral_de_comun;
        """
        return self._execute_geo_query(query, "comunas y corregimientos")

    def get_estaciones(self) -> gpd.GeoDataFrame:
        query = """
            SELECT 
                geom as geometry,
                estacion
            FROM shapes.estaciones;
        """
        return self._execute_geo_query(query, "estaciones")

    def _create_feature_collection(self, gdf: gpd.GeoDataFrame, property_mappings: Dict[str, str]) -> FeatureCollectionModel:
        features = []
        for _, row in gdf.iterrows():
            properties = {new_key: row[old_key] for new_key, old_key in property_mappings.items()}
            feature = FeatureModel(
                geometry=row.geometry.__geo_interface__,
                properties=properties
            )
            features.append(feature)
        return FeatureCollectionModel(features=features)

    def get_base_geometries(self) -> Dict[str, Any]:
        try:
            comunas_gdf = self.get_comunas_corregimientos()
            estaciones_gdf = self.get_estaciones()

            return {
                "comunas_corregimientos": self._create_feature_collection(
                    comunas_gdf,
                    {"nombre": "nombre"}
                ).to_dict(),
                "estaciones": self._create_feature_collection(
                    estaciones_gdf,
                    {"ESTACIO": "estacion"}
                ).to_dict()
            }

        except Exception as e:
            self.logger.error(f"Error en get_base_geometries: {str(e)}")
            raise

    def get_simplified_geometries(self, tolerance: float = 0.0001):
        try:
            comunas_gdf = self.get_comunas_corregimientos()
            estaciones_gdf = self.get_estaciones()

            comunas_gdf.geometry = comunas_gdf.geometry.simplify(tolerance)
            estaciones_gdf.geometry = estaciones_gdf.geometry.simplify(tolerance)

            return {
                "comunas_corregimientos": self._create_feature_collection(
                    comunas_gdf,
                    {"nombre": "nombre"}
                ),
                "estaciones": self._create_feature_collection(
                    estaciones_gdf,
                    {"estacion": "estacion"}
                )
            }
        except Exception as e:
            self.logger.error(f"Error en get_simplified_geometries: {str(e)}")
            raise