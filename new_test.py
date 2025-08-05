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


def export_image_to_drive(image, description, folder, scale=10, max_pixels=1e13, region=None):
    """Helper function to export an image to Google Drive with logging."""
    logger.info(f"Starting export task for {description}")

    task = ee.batch.Export.image.toDrive(
        image=image,
        description=description,
        folder=folder,
        scale=scale,
        maxPixels=max_pixels,
        region=region
    )

    task.start()
    logger.info(f"Export task {description} started (ID: {task.id})")
    return task

def main():
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

    max_cloud_cover = 100  # Maximum cloud cover percentage

    # 1. Load AOI and find matching tiles
    aoi = gpd.read_file(aoi_path, driver='GeoJSON')

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
    test_tiles = list(s2_ids)[:2]

    # Export each tile to Google Drive
    for i, tile_id in enumerate(test_tiles, 1):
        logger.info(f"Processing tile {i}/{len(test_tiles)}: {tile_id}")

        # Get the image
        image = ee.Image(f"COPERNICUS/S2_SR_HARMONIZED/{tile_id}")

        # Get the geometry (bounding box) of the image
        geometry = image.geometry()
        drive_folder = "SENTINEL_2_DATA_2"
        # Convert the geometry to a GeoJSON-like dictionary
        # Export parameters
        export_params = {
            'image': image,
            'description': f"S2_{tile_id}",
            'folder': drive_folder,
            'scale': 10,  # 10m resolution
            'region': geometry,
            'maxPixels': 1e13,
            'fileFormat': 'GeoTIFF',
            'formatOptions': {
                'cloudOptimized': True
            }
        }

        # Start the export
        task = ee.batch.Export.image.toDrive(**export_params)
        task.start()

        logger.info(f"Started export task for {tile_id} to folder {drive_folder}. Task ID: {task.id}")

    logger.info("all export tasks started, waiting for completion...")
    print('done')
    # TODO add exporting here


if __name__ == "__main__":
    main()