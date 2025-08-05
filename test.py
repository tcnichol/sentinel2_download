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
    test_tiles = set(list(s2_ids)[:2])


    collection_size = collection.size()
    print("The size of Sentinel-2 Image Collection:", collection_size.getInfo())
    image_list = collection.toList(collection_size)
    # for i in range(collection_size.getInfo()):
    #     image = ee.Image(image_list.get(i))
    #     date = image.date().format("YYYY-MM-dd").getInfo()
    #     cloud_percentage = image.get("CLOUDY_PIXEL_PERCENTAGE").getInfo()
    #     print(f"Image {i + 1}: Date={date}, Cloudy Pixel={cloud_percentage}%")

    # Create a list to store our export tasks
    export_tasks = []

    # Google Drive folder name
    drive_folder = "SENTINEL_2_TEST"
    # Export the first 2 images
    for i in range(min(2, collection_size.getInfo())):  # Ensure we don't exceed collection size
        image = ee.Image(image_list.get(i))
        date = image.date().format("YYYY-MM-dd").getInfo()
        cloud_percentage = image.get("CLOUDY_PIXEL_PERCENTAGE").getInfo()
        image_id = image.get("system:index").getInfo()

        logger.info(f"Preparing to export image {i + 1}: ID={image_id}, Date={date}, Cloud={cloud_percentage}%")

        # Select the bands you want to export (here using all bands)
        image_to_export = image.select(['B2', 'B3', 'B4', 'B8'])  # Add/remove bands as needed

        # Create a description for the export task
        task_description = f"S2_{image_id}_{date}"

        # Get the geometry for the export region
        region = image.geometry().bounds().getInfo()['coordinates']

        # Start the export task
        task = export_image_to_drive(
            image=image_to_export,
            description=task_description,
            folder=drive_folder,
            scale=10,  # 10m resolution
            region=region
        )

        export_tasks.append(task)

        # Add a small delay between task starts to avoid rate limiting
        time.sleep(1)

    logger.info(f"Started {len(export_tasks)} export tasks to Google Drive folder '{drive_folder}'")

    # Print task statuses
    for i, task in enumerate(export_tasks, 1):
        logger.info(
            f"Task {i}: ID={task.id}, Description={task.config['description']}, Status={task.status()['state']}")

    print("done")


    print("done")

    # TODO add exporting here


if __name__ == "__main__":
    main()