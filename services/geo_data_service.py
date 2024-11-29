from shapely import wkb
import geopandas as gpd
from typing import Dict, Optional, Any
import logging
from dataclasses import dataclass
from db.athena_rds_client import ejecutar_query_rds
#from utils.athena_rds_client_ssh import ejecutar_query_rds
# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
        self.logger = logging.getLogger(__name__)

    def _execute_geo_query(self, query: str, entity_name: str) -> Optional[gpd.GeoDataFrame]:
        """
        Ejecuta una consulta geográfica y devuelve un GeoDataFrame.
        """
        try:
            result = ejecutar_query_rds(query)
            if result is None:
                raise ValueError(f"No se obtuvieron datos para {entity_name}")

            # Convertir la columna geometry de WKB a objetos shapely
            result['geometry'] = result['geometry'].apply(
                lambda x: wkb.loads(bytes.fromhex(x))
            )
            return gpd.GeoDataFrame(result, geometry='geometry')

        except Exception as e:
            self.logger.error(f"Error al obtener {entity_name}: {str(e)}")
            raise

    def get_comunas_corregimientos(self) -> gpd.GeoDataFrame:
        """
        Obtiene GeoDataFrame de comunas y corregimientos desde RDS PostgreSQL.
        """
        query = """
            SELECT 
                geom as geometry,
                nombre
            FROM shapes.limite_catastral_de_comun;
        """
        return self._execute_geo_query(query, "comunas y corregimientos")

    def get_estaciones(self) -> gpd.GeoDataFrame:
        """
        Obtiene GeoDataFrame de estaciones desde RDS PostgreSQL.
        """
        query = """
            SELECT 
                geom as geometry,
                estacion
            FROM shapes.estaciones;
        """
        return self._execute_geo_query(query, "estaciones")

    def _create_feature_collection(self,
                                   gdf: gpd.GeoDataFrame,
                                   property_mappings: Dict[str, str]) -> GeoJSONFeatureCollection:
        """
        Crea una colección de features GeoJSON a partir de un GeoDataFrame.
        """
        feature_collection = GeoJSONFeatureCollection()

        for _, row in gdf.iterrows():
            properties = {
                new_key: row[old_key]
                for new_key, old_key in property_mappings.items()
            }
            feature_collection.add_feature(
                geometry=row.geometry.__geo_interface__,
                properties=properties
            )

        return feature_collection

    def get_base_geometries(self) -> Dict[str, Any]:
        """
        Obtiene y formatea todas las geometrías base.
        """
        try:
            # Obtener datos
            comunas_gdf = self.get_comunas_corregimientos()
            estaciones_gdf = self.get_estaciones()

            # Crear colecciones de features
            comunas_collection = self._create_feature_collection(
                comunas_gdf,
                {"nombre": "nombre"}
            )

            estaciones_collection = self._create_feature_collection(
                estaciones_gdf,
                {"ESTACIO": "estacion"}
            )

            return {
                "comunas_corregimientos": comunas_collection.to_dict(),
                "estaciones": estaciones_collection.to_dict()
            }

        except Exception as e:
            self.logger.error(f"Error en get_base_geometries: {str(e)}")
            raise

    def get_simplified_geometries(self, tolerance: float = 0.0001) -> Dict[str, Any]:
        """
        Obtiene geometrías simplificadas para mejor rendimiento.
        """
        try:
            # Obtener datos
            comunas_gdf = self.get_comunas_corregimientos()
            estaciones_gdf = self.get_estaciones()

            # Simplificar geometrías
            comunas_gdf.geometry = comunas_gdf.geometry.simplify(tolerance)
            estaciones_gdf.geometry = estaciones_gdf.geometry.simplify(tolerance)

            # Crear colecciones de features
            comunas_collection = self._create_feature_collection(
                comunas_gdf,
                {"nombre": "nombre"}
            )

            estaciones_collection = self._create_feature_collection(
                estaciones_gdf,
                {"ESTACIO": "estacion"}
            )

            return {
                "comunas_corregimientos": comunas_collection.to_dict(),
                "estaciones": estaciones_collection.to_dict()
            }

        except Exception as e:
            self.logger.error(f"Error en get_simplified_geometries: {str(e)}")
            raise