from pathlib import Path
import geopandas as gpd
import shapely
import folium
import seaborn as sn

gpd_request = (gpd.GeoDataFrame.from_file("tiles_nwt_2010_2016.geojson")
               .dissolve().explode(ignore_index=True,index_parts=True)
               ).drop(columns="tile_name")
gpd_request["name"] = gpd_request.index.map(lambda x: f"{x:02d}")
display(gpd_request)
gpd_request.explore()

s2_coverage = gpd.read_parquet("Sentinel2_Tiles_Arctic.parquet")
s2_coverage


s2_tiles = s2_coverage.sjoin(gpd_request.to_crs(4326)).drop_duplicates("Name")

minx, miny, maxx, maxy = s2_tiles.total_bounds
m = folium.Map()

s2_tiles.assign(utm=s2_tiles.Name.str[0:2]).explore(m=m, column="utm", attr="Name")
gpd_request.explore(m=m, color="red")
m.fit_bounds([[miny,minx],[maxy,maxx]])
m

s2_ids = [s2name for s2name in s2_tiles.Name if s2name.startswith("08")] # only UTM08
s2_ids = list(set(s2_ids) - {"08WMA"}) # 08WMA is not necessary
s2_ids

import ee
ee.Initialize()

def map_ic_to_fc(img:ee.Image):
    return ee.Feature(img.geometry(),{
        "ee_id" : img.id(),
        "ee_timestamp" : img.get("system:time_start"),
        "tile_id" : img.get("MGRS_TILE"),
        "ee_cpp" : img.get("CLOUDY_PIXEL_PERCENTAGE"),
        "ee_datatake": img.get("DATATAKE_IDENTIFIER"),
        "ee_quality" : img.get("GENERAL_QUALITY")
    })

ee_ic = ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED").filter(ee.Filter.And(
    ee.Filter.inList("MGRS_TILE", s2_ids ),
    ee.Filter.calendarRange(6,9,"month"),
    ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 10)
))

ee_fc = ee.FeatureCollection(  ee_ic.map(map_ic_to_fc) )

gdf_ee_s2ftrs = ee.data.computeFeatures({
    "expression" : ee_fc,
    "fileFormat" : "GEOPANDAS_GEODATAFRAME"
}).set_crs(4326)

gdf_ee_s2ftrs

gdf_ee_s2ftrs.ee_id

import geedim as gd

def download_with_basepath(s2_id, basepath):
    download_folder = Path(basepath) / s2_id
    download_folder.mkdir( parents=True,exist_ok=True )

    download_filepath = download_folder / f"{s2_id}_SR.tif"
    sr_ee_img = ee.Image("COPERNICUS/S2_SR_HARMONIZED/"+s2_id).select(["B2","B3","B4","B8"])
    gd_sr_img = gd.download.BaseImage(sr_ee_img)
    gd_sr_img.download( download_filepath, dtype='uint16', overwrite=True)

    scl_download_filepath = download_folder / f"{s2_id}_SCL.tif"
    scl_ee_img = ee.Image("COPERNICUS/S2_SR_HARMONIZED/"+s2_id).select(["SCL"])
    gd_scl_img = gd.download.BaseImage(scl_ee_img)
    gd_scl_img.download( scl_download_filepath, dtype='uint16', overwrite=True)

gdf_ee_s2ftrs.ee_id.apply(download_with_basepath, basepath = Path(r"C:\Temp"))