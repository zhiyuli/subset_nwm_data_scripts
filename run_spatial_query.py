import pyspatialite.dbapi2 as db

db_epsg = "4269"


###################### input WKT #######################
#input_wkt = "POLYGON((500000 4316776.583097936,500000 4427757.218624833,585360.4618433624 4428236.064519553,586592.6780279021 4317252.164517585,500000 4316776.583097936))"
#input_epsg = "26912"

#input_wkt = "POLYGON((-111 38.99999999999998,-111 39.99999999999998,-109.9999999999998 39.99999999999992,-109.9999999999998 38.99999999999992,-111 38.99999999999998))"
#input_epsg = "4269"

#input_geom='ST_Transform(PolygonFromText("{input_wkt}", {input_epsg}), {db_epsg})'.format(input_wkt=input_wkt, input_epsg=input_epsg, db_epsg=db_epsg)

###################### input GeoJSON #######################
input_geojson = '{"type":"Polygon",' \
               '"coordinates":[[[500000, 4316776.583097936],[500000, 4427757.218624833],' \
               '[585360.4618433624, 4428236.064519553],[586592.6780279021, 4317252.164517585],[500000, 4316776.583097936]]]}'
input_epsg = "26912"
input_geom="ST_Transform(SetSRID(GeomFromGeoJSON('{input_geojson}'), {input_epsg}), {db_epsg})".format(input_geojson=input_geojson, input_epsg=input_epsg, db_epsg=db_epsg)


############################## Query Grid Index
db_file = "/home/drew/Desktop/nwm.sqlite"
sql_str = 'SELECT min(Grid.west_east) AS minX,\
max(Grid.west_east) AS maxX, \
min(Grid.south_nort) AS minY, \
max(Grid.south_nort) AS maxY \
FROM grid_polygon_wgs84 as Grid \
WHERE \
ST_Intersects(Grid.geometry, {input_geom}) = 1 \
and \
Grid.ROWID IN \
(\
SELECT ROWID FROM SpatialIndex WHERE f_table_name = "grid_polygon_wgs84" \
AND search_frame = {input_geom}\
);'.format(input_geom=input_geom)

############################## Query Stream comid
# db_file = "/home/drew/Desktop/nwm2.sqlite"
# sql_str = 'SELECT count(station_id) \
# FROM stream \
# WHERE \
# ST_Intersects(stream.geometry, {input_geom}) = 1 \
# and \
# stream.ROWID IN \
# (\
# SELECT ROWID FROM SpatialIndex WHERE f_table_name = "stream" \
# AND search_frame = {input_geom}\
# );'.format(input_geom=input_geom)


print sql_str
con = db.connect(db_file)
cursor = con.execute(sql_str)
print cursor.fetchall()

con.close()
