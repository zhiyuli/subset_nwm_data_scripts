#gdaltransform -s_srs EPSG:26916 -t_srs EPSG:3857
#446495 3681410
import pycrs
import pyproj
from osgeo import osr

# esri_prj_str_nad83_utm_zone16 = 'PROJCS["NAD_1983_UTM_Zone_16N",GEOGCS["GCS_North_American_1983",DATUM["D_NORTH_AMERICAN_1983",SPHEROID["GRS_1980",6378137,298.257222101]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]],PROJECTION["Transverse_Mercator"],PARAMETER["latitude_of_origin",0],PARAMETER["central_meridian",-87],PARAMETER["scale_factor",0.9996],PARAMETER["false_easting",500000],PARAMETER["false_northing",0],UNIT["Meter",1]]'
# wkt_prj_str_nad83_utm_zone_16 = 'PROJCS["NAD_1983_UTM_Zone_16N",GEOGCS["GCS_North_American_1983",DATUM["North_American_Datum_1983",SPHEROID["GRS_1980",6378137,298.257222101]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]],PROJECTION["Transverse_Mercator"],PARAMETER["latitude_of_origin",0],PARAMETER["central_meridian",-87],PARAMETER["scale_factor",0.9996],PARAMETER["false_easting",500000],PARAMETER["false_northing",0],UNIT["Meter",1]]'
#
# #fromcrs = pycrs.parser.from_unknown_text(esri_prj_str_nad83_utm_zone16)
# fromcrs = pycrs.parser.from_esri_wkt(esri_prj_str_nad83_utm_zone16)
#
# srs = osr.SpatialReference()
# srs.ImportFromESRI([esri_prj_str_nad83_utm_zone16])
#
# prj_26916_gdal_wkt = srs.ExportToWkt()
#
# print prj_26916_gdal_wkt == esri_prj_str_nad83_utm_zone16
# prj_26916_gdal = pyproj.Proj(srs.ExportToProj4())
# prj_26916_epsg = pyproj.Proj("+init=EPSG:26916")
# prj_26916_proj4 = pyproj.Proj(fromcrs.to_proj4())
# prj_3857 = pyproj.Proj("+init=EPSG:3857")
#
# print pyproj.transform(prj_26916_epsg, prj_3857, 446495, 3681410)
# print pyproj.transform(prj_26916_gdal, prj_3857, 446495, 3681410)
# print pyproj.transform(prj_26916_proj4, prj_3857, 446495, 3681410)



import pycrs
import pyproj

esri_prj_str_nad83_utm_zone16 = 'PROJCS["NAD_1983_UTM_Zone_16N",GEOGCS["GCS_North_American_1983",DATUM["D_NORTH_AMERICAN_1983",SPHEROID["GRS_1980",6378137,298.257222101]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]],PROJECTION["Transverse_Mercator"],PARAMETER["latitude_of_origin",0],PARAMETER["central_meridian",-87],PARAMETER["scale_factor",0.9996],PARAMETER["false_easting",500000],PARAMETER["false_northing",0],UNIT["Meter",1]]'
fromcrs = pycrs.parser.from_esri_wkt(esri_prj_str_nad83_utm_zone16)
proj4_str = fromcrs.to_proj4()
print proj4_str
proj4_str = proj4_str.replace("tmerc", "etmerc")
print proj4_str
prj_obj_from_proj4 = pyproj.Proj(proj4_str)


prj_obj_26916_from_epsg = pyproj.Proj("+init=EPSG:26916")
prj_obj_3857_from_epsg = pyproj.Proj("+init=EPSG:3857")

# (-9748715.328601453, 3929480.5837150216)
print pyproj.transform(prj_obj_from_proj4, prj_obj_3857_from_epsg, 446495, 3681410)

# (-9748750.584671738, 3931248.690782473)
print pyproj.transform(prj_obj_26916_from_epsg, prj_obj_3857_from_epsg, 446495, 3681410)

#gdaltransform -s_srs EPSG:26916 -t_srs EPSG:3857
#446495 3681410
#-9748750.58467174 3931248.69078237