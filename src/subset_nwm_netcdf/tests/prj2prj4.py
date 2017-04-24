# from osgeo import osr
#
# srs = osr.SpatialReference()
# wkt_text = 'PROJCS["Sphere_Lambert_Conformal_Conic",GEOGCS["GCS_Sphere",DATUM["D_Sphere",SPHEROID["Sphere",6370000.0,0.0]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Lambert_Conformal_Conic"],PARAMETER["false_easting",0.0],PARAMETER["false_northing",0.0],PARAMETER["central_meridian",-97.0],PARAMETER["standard_parallel_1",30.0],PARAMETER["standard_parallel_2",60.0],PARAMETER["latitude_of_origin",40.0000076294],UNIT["Meter",1.0]]'
# # Imports WKT to Spatial Reference Object
# srs.ImportFromWkt(wkt_text)
# # print srs.MorphToESRI() # converts the WKT to an ESRI-compatible format
# # print srs.ExportToWkt()
#
# print srs.ExportToProj4()

import pycrs
fromcrs = pycrs.loader.from_file("../static/data/tests/Sphere_Lambert_Conformal_Conic.prj")
print "esri_wkt"
print fromcrs.to_esri_wkt()
print "ogc_wkt"
print fromcrs.to_ogc_wkt()

fromcrs_proj4 = fromcrs.to_proj4()
print "proj4"
print fromcrs_proj4


# p=pycrs.parser.from_epsg_code(4269)
# print p.to_proj4()

"INSERT into spatial_ref_sys (srid, auth_name, auth_srid, ref_sys_name, proj4text) values (33333,'CUSTOM',33333,'NWM_Lambert_Conformal_Conic','+proj=lcc +a=6370000.0 +f=0.0 +pm=0.0  +x_0=0.0 +y_0=0.0 +lon_0=-97.0 +lat_1=30.0 +lat_2=60.0 +lat_0=40.0000076294 +units=m +axis=enu +no_defs');"

