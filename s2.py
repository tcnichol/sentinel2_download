import logging
import os
from pathlib import Path
from typing import Set, Dict, Optional
import concurrent.futures
import ee
import geopandas as gpd
import xarray as xr
from tilecache import XarrayCacheManager
from earthengine import init_ee, init_ee_from_credentials
import time
# Initialize logging
import requests
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import json
from shapely.geometry import shape


def load_geojson_directly(path: Path) -> gpd.GeoDataFrame:
    with open(path) as f:
        data = json.load(f)

    features = []
    for feature in data['features']:
        features.append({
            'tile_name': feature['properties']['tile_name'],
            'geometry': shape(feature['geometry'])
        })

    return gpd.GeoDataFrame(
        features,
        crs=data['crs']['properties']['name']  # "urn:ogc:def:crs:EPSG::3413"
    )

class Sentinel2GEEExporter:
    def __init__(self, cache_dir: Path = Path("gee_cache"), max_workers: int = 4):
        self.cache_dir = cache_dir
        self.max_workers = max_workers
        self.cache_dir.mkdir(exist_ok=True)
        # ee.Initialize()

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
            max_retries: int = 3,
            chunk_size: int = 8192
    ) -> Optional[Path]:
        """Download Sentinel-2 tile using EE's getDownloadURL with improved error handling."""
        cache_file = self.cache_dir / f"{s2_id}.tif"

        if cache_file.exists():
            logger.debug(f"Skipping {s2_id} (already cached)")
            return cache_file

        for attempt in range(max_retries):
            try:
                logger.info(f"Attempt {attempt + 1} for {s2_id}")

                # Get the image and select bands
                img = ee.Image(f"COPERNICUS/S2_SR_HARMONIZED/{s2_id}")
                img = img.select(list(bands_mapping.keys()))

                # Get the native projection
                projection = img.select(0).projection()
                scale = projection.nominalScale().getInfo()

                # Get download URL with native projection
                url = img.getDownloadURL({
                    'name': f'S2_{s2_id}',
                    'scale': scale,
                    'crs': projection.crs(),
                    'region': img.geometry(),
                    'filePerBand': False,
                    'format': 'GEO_TIFF'
                })

                # Download with streaming and timeout
                with requests.get(url, stream=True, timeout=30) as response:
                    response.raise_for_status()

                    # Save to temporary file first
                    temp_file = cache_file.with_suffix('.tmp')
                    with open(temp_file, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=chunk_size):
                            if chunk:  # filter out keep-alive chunks
                                f.write(chunk)

                    # Rename temp file to final name after successful download
                    temp_file.rename(cache_file)

                    logger.info(f"Successfully downloaded {s2_id} to {cache_file}")
                    return cache_file

            except requests.exceptions.RequestException as e:
                logger.warning(f"Download failed (attempt {attempt + 1}): {str(e)}")
                if attempt == max_retries - 1:
                    logger.error(f"Max retries exceeded for {s2_id}")
                    return None
                time.sleep(5 * (attempt + 1))  # Exponential backoff

            except Exception as e:
                logger.error(f"Unexpected error processing {s2_id}: {str(e)}")
                return None

        return None

    def export_tiles(
            self,
            s2_ids: Set[str],
            bands_mapping: Dict[str, str] = {"B2": "blue", "B3": "green", "B4": "red", "B8": "nir"},
    ) -> Dict[str, Path]:
        """Download multiple tiles in parallel with improved progress tracking."""
        results = {}
        total = len(s2_ids)

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self._process_single_tile, s2_id, bands_mapping): s2_id
                for s2_id in s2_ids
            }

            for i, future in enumerate(concurrent.futures.as_completed(futures), 1):
                s2_id = futures[future]
                try:
                    result = future.result()
                    if result:
                        results[s2_id] = result
                    logger.info(f"Progress: {i}/{total} tiles processed")
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

def download(aoi_path, start_date, end_date, cache_dir, max_cloud_cover=20, max_workers=4):

    # --- Configuration ---

    # --- Execution ---
    exporter = Sentinel2GEEExporter(cache_dir, max_workers)

    # 1. Load AOI and find matching tiles
    aoi = gpd.read_file(aoi_path, driver='GeoJSON')

    # Validate the GeoDataFrame
    if not isinstance(aoi, gpd.GeoDataFrame):
        raise ValueError("AOI file did not load as a GeoDataFrame")
    if aoi.empty:
        raise ValueError("AOI file contains no features")

    logger.info(f"Loaded AOI with {len(aoi)} features from {aoi_path}")

    s2_ids = exporter.get_s2_tile_ids(aoi, start_date, end_date, max_cloud_cover)

    # 2. Download all tiles
    results = exporter.export_tiles(s2_ids)

    logger.info(f"Successfully downloaded {len(results)} tiles to {cache_dir}")


if __name__ == "__main__":

    credentials_path = os.path.join(os.getcwd(), 'credentials.json')
    if os.path.exists(credentials_path):
        print("We have credentials.json, initializing Earth Engine with it")
        init_ee_from_credentials(credentials_path=Path(credentials_path),
                                 project="uiuc-ncsa-permafrost",
                                 use_highvolume=True)
    else:
        print("We do not have credentails, do nothing")

    aoi_location = os.path.join(os.getcwd(), 'sample', 'tiles_nwt_2010_2016_extra.geojson')
    print(os.path.exists(aoi_location), 'the file exists')
    aoi_path = Path(aoi_location)  # GeoJSON file with AOI polygons
    start_date = "2024-07"  # YYYY-MM-DD
    end_date = "2024-09"  # YYYY-MM-DD
    cache_location = os.path.join(os.getcwd(), 'cache')
    cache_dir = Path(cache_location)

    try:
        download(aoi_path=aoi_location,
                 start_date=start_date,
                 end_date=end_date,
                 cache_dir=cache_dir)
    except Exception as e:
        print("An error occurred during download:", e)

    # try:
    #     init_ee(project="uiuc-ncsa-permafrost", use_highvolume=True)
    # except Exception as e:
    #     print("Failed to initialize Earth Engine:", e)

    # main()