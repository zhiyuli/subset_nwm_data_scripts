import pyspatialite.dbapi2 as db


# NAD83 <--> WGS84 http://tagis.dep.wv.gov/convert/
# WGS84: 4326  X: Lon, Y: Lat
# NAD83: 4269  X: Lon, Y: Lat
# UTM NAD83 Zone 12 (for Utah): 26912  X: 6 digits, Y: 7 digits
# Web Mercator: 3857

db_epsg = "4269"  # NAD83


###################### input WKT #######################
#input_wkt = "POLYGON((500000 4316776.583097936,500000 4427757.218624833,585360.4618433624 4428236.064519553,586592.6780279021 4317252.164517585,500000 4316776.583097936))"
#input_epsg = "26912"

#input_wkt = "POLYGON((-111 38.99999999999998,-111 39.99999999999998,-109.9999999999998 39.99999999999992,-109.9999999999998 38.99999999999992,-111 38.99999999999998))"
#input_epsg = "4269"

#input_geom='ST_Transform(PolygonFromText("{input_wkt}", {input_epsg}), {db_epsg})'.format(input_wkt=input_wkt, input_epsg=input_epsg, db_epsg=db_epsg)

###################### input GeoJSON #######################
# ## Utah UTM-NAD83 Zone 12
# input_geojson = '{"type":"Polygon",' \
#                '"coordinates":[[[354809.3,4491461.1],' \
#                '[491794.4,4490284.0],[499849.4,4378693.3],[345451.1,4391172.0],[354809.3,4491461.1]]]}'
# input_epsg = "26912"

# ## Utah WGS84
# input_geojson = '{"type":"Polygon",' \
#                '"coordinates":[[[-112.715023, 40.561205],[-111.096942,40.563292],' \
#                '[-111.001764,39.557933],[-112.801520,39.656397],[-112.715023, 40.561205]]]}'
# input_epsg = "4326"

# across USA
input_geojson = '{"type":"Polygon",' \
               '"coordinates":[[[-109.436035, 43.011979],[-93.361976,42.599528],' \
               '[-92.768481,34.295595],[-111.418367,34.515505],[-109.436035, 43.011979]]]}'
input_epsg = "4326"


input_geom="ST_Transform(SetSRID(GeomFromGeoJSON('{input_geojson}'), {input_epsg}), {db_epsg})".format(input_geojson=input_geojson, input_epsg=input_epsg, db_epsg=db_epsg)
db_file = "/home/drew/Desktop/nwm.sqlite"

############################## Query Grid Index

sql_grid_land = 'SELECT min(Grid.west_east) AS minX,\
max(Grid.west_east) AS maxX, \
min(Grid.south_north) AS minY, \
max(Grid.south_north) AS maxY \
FROM grid_land as Grid \
WHERE \
ST_Intersects(Grid.Shape, {input_geom}) = 1 \
and \
Grid.ROWID IN \
(\
SELECT ROWID FROM SpatialIndex WHERE f_table_name = "grid_land" \
AND search_frame = {input_geom}\
);'.format(input_geom=input_geom)

############################## Query Stream comid

sql_stream = 'SELECT station_id \
FROM stream \
WHERE \
ST_Intersects(stream.Shape, {input_geom}) = 1 \
and \
stream.ROWID IN \
(\
SELECT ROWID FROM SpatialIndex WHERE f_table_name = "stream" \
AND search_frame = {input_geom}\
);'.format(input_geom=input_geom)

con = db.connect(db_file)

cursor = con.execute(sql_grid_land)

grid_indices = list(cursor.fetchone())

cursor = con.execute(sql_stream)
stream_comids = [item[0].encode('ascii', 'ignore') for item in cursor.fetchall()]

con.close()

rslt = {}
rslt['grid_land'] = {"minX": grid_indices[0], "maxX": grid_indices[1],
                     "minY": grid_indices[2], "maxY": grid_indices[3],}
rslt['stream'] = {"count": len(stream_comids), "comids": stream_comids}


print rslt["grid_land"], rslt["stream"]["count"]