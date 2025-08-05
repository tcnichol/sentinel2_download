import logging
from pathlib import Path
from typing import Set, Dict, Optional
import concurrent.futures
import ee
import geopandas as gpd
import xarray as xr
from darts_utils.tilecache import XarrayCacheManager

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Sentinel2GEEExporter:
    def __init__(self, cache_dir: Path = Path("gee_cache"), max_workers: int = 4):
        self.cache_dir = cache_dir
        self.max_workers = max_workers
        self.cache_dir.mkdir(exist_ok=True)
        ee.Initialize()

    def get_s2_tile_ids(
            self,
            aoi: gpd.GeoDataFrame,
            start_date: str,
            end_date: str,
            max_cloud_cover: int = 20,
    ) -> Set[str]:
        """Fetch Sentinel-2 tile IDs overlapping the AOI and date range."""
        aoi = aoi.to_crs("EPSG:4326")  # GEE requires WGS84
        s2_ids = set()

        for _, row in aoi.iterrows():
            geom = ee.Geometry.Polygon(list(row.geometry.exterior.coords))
            collection = (
                ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
                .filterBounds(geom)
                .filterDate(start_date, end_date)
                .filterMetadata("CLOUDY_PIXEL_PERCENTAGE", "less_than", max_cloud_cover)
            )
            s2_ids.update(collection.aggregate_array("system:index").getInfo())

        logger.info(f"Found {len(s2_ids)} Sentinel-2 tiles.")
        return s2_ids

    def _process_single_tile(
            self,
            s2_id: str,
            bands_mapping: Dict[str, str] = {"B2": "blue", "B3": "green", "B4": "red", "B8": "nir"},
    ) -> Optional[Path]:
        """Download and process a single Sentinel-2 tile."""
        cache_file = self.cache_dir / f"gee-s2srh-{s2_id}.nc"
        if cache_file.exists():
            logger.debug(f"Skipping {s2_id} (already cached)")
            return None

        try:
            img = ee.Image(f"COPERNICUS/S2_SR_HARMONIZED/{s2_id}")
            img = img.select(list(bands_mapping.keys()))

            ds = xr.open_dataset(
                img,
                engine="ee",
                geometry=img.geometry(),
                crs=img.select(0).projection().crs().getInfo(),
                scale=10,
            ).load()

            # Post-processing
            ds = (
                ds.isel(time=0)
                .drop_vars("time")
                .rename({"X": "x", "Y": "y"})
                .transpose("y", "x")
                .rename(bands_mapping)
            )
            ds = ds.odc.assign_crs(ds.attrs["crs"])

            # Save to cache
            ds.to_netcdf(cache_file)
            logger.info(f"Exported {s2_id} to {cache_file}")
            return cache_file

        except Exception as e:
            logger.error(f"Failed to process {s2_id}: {str(e)}")
            return None

    def export_tiles(
            self,
            s2_ids: Set[str],
            bands_mapping: Dict[str, str] = {"B2": "blue", "B3": "green", "B4": "red", "B8": "nir"},
    ) -> Dict[str, Path]:
        """Download multiple tiles in parallel."""
        results = {}

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self._process_single_tile, s2_id, bands_mapping): s2_id
                for s2_id in s2_ids
            }

            for future in concurrent.futures.as_completed(futures):
                s2_id = futures[future]
                try:
                    result = future.result()
                    if result:
                        results[s2_id] = result
                except Exception as e:
                    logger.error(f"Error processing {s2_id}: {str(e)}")

        return results


def main():
    # --- Configuration ---
    aoi_path = Path("path/to/your/aoi.geojson")  # GeoJSON file with AOI polygons
    start_date = "2023-01-01"  # YYYY-MM-DD
    end_date = "2023-01-31"  # YYYY-MM-DD
    cache_dir = Path("sentinel2_gee_cache")
    max_cloud_cover = 20  # Percentage
    max_workers = 4  # Parallel downloads

    # --- Execution ---
    exporter = Sentinel2GEEExporter(cache_dir, max_workers)

    # 1. Load AOI and find matching tiles
    aoi = gpd.read_file(aoi_path)
    s2_ids = exporter.get_s2_tile_ids(aoi, start_date, end_date, max_cloud_cover)

    # 2. Download all tiles
    results = exporter.export_tiles(s2_ids)

    logger.info(f"Successfully downloaded {len(results)} tiles to {cache_dir}")


if __name__ == "__main__":
    main()