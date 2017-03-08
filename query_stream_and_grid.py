import os
import json
import fiona
import shapely.wkt
import shapely.geometry
import pyspatialite.dbapi2 as db
import exceptions

# db file full path
db_file = "/home/drew/Desktop/nwm.sqlite"
# the default espg code in db
db_epsg = 4269

def query_comids_and_grid_indices(query_type="shapefile", shp_path=None, geom_str=None,
                                        in_epsg=None, huc_id=None, stream_list=[]):

    '''

    :param in_type: shapefile, wkt, geojson, huc_8, huc_10, huc_12, stream
    :param shp_path: full path to *.shp file
    :param geom_str: geojson str or wkt str
    :param in_epsg: projection of shapefile or geom_str
    :param huc_id: huc id (huc 8 ,10 ,12)
    :param stream_list: list of stream comid
    :return:
    '''
    in_epsg_checked = None
    query_type_lower = query_type.lower()
    query_stream = True
    query_grid = True
    query_reservoir = True
    if query_type_lower in ["shapefile", "geojson", "wkt"]:

        shape_obj, in_epsg_checked = get_shapely_shape_obj(query_type_lower=query_type_lower, in_epsg=in_epsg,
                                                           shp_path=shp_path, geom_str=geom_str)
    elif "huc" in query_type_lower:

        shape_obj, in_epsg_checked = get_huc_bbox_shapely_shape_obj(huc_type=query_type_lower,
                                                                    huc_id=huc_id)
    elif "stream" == query_type_lower:
        raise exceptions.NotImplementedError()

    # get the exterior of first polygon
    if shape_obj.geom_type.lower() == "multipolygon":
        polygon_exterior_linearring = shape_obj[0].exterior
    elif shape_obj.geom_type.lower() == "polygon":
        polygon_exterior_linearring = shape_obj.exterior
    else:
        raise Exception("Input Geometry is not Polygon")

    polygon_query_window = shapely.geometry.Polygon(polygon_exterior_linearring)
    data = perform_spatial_query(query_window_wkt=polygon_query_window.wkt,
                                 input_epsg=in_epsg_checked,
                                 query_stream=query_stream,
                                 query_grid=query_grid,
                                 query_reservoir=query_reservoir)

    print data["grid_land"], data["stream"]["count"], data["reservoir"]["count"]

    pass


def get_shapely_shape_obj(query_type_lower=None, in_epsg=None, shp_path=None, geom_str=None):

    if query_type_lower is None:
        raise Exception("Parameter 'query_type_lower' is not given")

    if in_epsg is not None and not check_supported_epsg(epsg=in_epsg, db_file=db_file):
        raise Exception("A invalid/unsupported epsg code is given")

    in_epsg_checked = in_epsg

    if query_type_lower == "shapefile":
        if shp_path is None or not os.path.exists(shp_path):
            raise Exception("Shp path is invalid")
        shp_obj = fiona.open(shp_path)

        if in_epsg_checked is None:
            # check shapefile prj for epsg
            print "No epsg code is given. Trying to extract epsg code from shapefile...."
            if "init" in shp_obj.crs:
                epsg = shp_obj.crs["init"].split(":")[1]
                if check_supported_epsg(epsg=epsg, db_file=db_file):
                    in_epsg_checked = int(epsg)
                    print "epsg:", in_epsg_checked
                else:
                    raise Exception("Shapefile has unsupported projection/epsg code ")
            else:
                raise Exception("Shapefile has no or invalid projection/epsg code")

        # parse shp geom
        # check gometry type
        geom_type = shp_obj.schema['geometry'].lower()
        if geom_type not in ["polygon", "multipolygon"]:
            raise Exception("Shapefile must be type of Polygon or MultiPolygon")

        first_feature_obj = next(shp_obj)
        shape_obj = shapely.geometry.shape(first_feature_obj["geometry"])

        # geojson = shapely.geometry.mapping(shape_obj)
        # wkt_str = shape_obj.wkt

    elif query_type_lower == "geojson":

        if in_epsg_checked is None:
            raise Exception("No epsg code is given")
        geojson = json.loads(geom_str)
        shape_obj = shapely.geometry.asShape(geojson)

    else:  # wkt

        if in_epsg_checked is None:
            raise Exception("No epsg code is given")

        shape_obj = shapely.wkt.loads(geom_str)

    return shape_obj, in_epsg_checked


def check_supported_epsg(epsg=None, db_file=None):

    conn = None
    try:
        sql_str = "select count(*) from spatial_ref_sys where auth_srid = {0}".format(epsg)
        conn = db.connect(db_file)
        cursor = conn.execute(sql_str)
        epsg_count = int(cursor.fetchone()[0])
        if epsg_count > 0:
            return True
        return False

    except Exception as ex:
        raise ex
    finally:
        if conn is not None:
            conn.close()


def perform_spatial_query(query_window_wkt=None,
                          input_epsg=None,
                          query_stream=True,
                          query_grid=True,
                          query_reservoir=True):

    conn = None
    data = {"status": "success"}

    try:
        input_geom = "ST_Transform(GeomFromText('{input_wkt}', {input_epsg}), {db_epsg})".format(
            input_wkt=query_window_wkt, input_epsg=input_epsg, db_epsg=db_epsg)

        sql_template = 'SELECT {query_string} \
                    FROM {query_table_name} \
                    WHERE \
                    ST_Intersects({query_table_name}.{geometry_field_name}, {input_geom}) = 1 \
                    and \
                    {query_table_name}.ROWID IN \
                    (\
                    SELECT ROWID FROM SpatialIndex WHERE f_table_name = "{query_table_name}" \
                    AND search_frame = {input_geom}\
                    );'

        conn = db.connect(db_file)
        geometry_field_name = 'Shape'

        if query_grid:
            query_table_name = 'grid_land'
            query_string = 'min(grid_land.west_east) AS minX, '\
                           'max(grid_land.west_east) AS maxX, '\
                           'min(grid_land.south_north) AS minY, '\
                           'max(grid_land.south_north) AS maxY'

            sql_grid_land = sql_template.format(query_table_name=query_table_name,
                                                query_string=query_string,
                                                geometry_field_name=geometry_field_name,
                                                input_geom=input_geom)

            cursor = conn.execute(sql_grid_land)
            grid_indices = list(cursor.fetchone())
            data['grid_land'] = {"minX": grid_indices[0], "maxX": grid_indices[1],
                                 "minY": grid_indices[2], "maxY": grid_indices[3]}
        if query_stream:
            query_table_name = 'stream'
            query_string = 'station_id'

            sql_stream = sql_template.format(query_table_name=query_table_name,
                                             query_string=query_string,
                                             geometry_field_name=geometry_field_name,
                                             input_geom=input_geom)
            cursor = conn.execute(sql_stream)

            stream_comids = [item[0] for item in cursor.fetchall()]
            data['stream'] = {"count": len(stream_comids), "comids": stream_comids}

        if query_reservoir:
            query_table_name = 'reservior'
            query_string = 'COMID'

            sql_reservoir = sql_template.format(query_table_name=query_table_name,
                                                query_string=query_string,
                                                geometry_field_name=geometry_field_name,
                                                input_geom=input_geom)

            cursor = conn.execute(sql_reservoir)
            reservior_comids = [item[0] for item in cursor.fetchall()]
            data['reservoir'] = {"count": len(reservior_comids), "comids": reservior_comids}

            conn.close()

        return data

    except Exception as ex:
        data["status": "error"]
        raise ex
    finally:
        if conn is not None:
            conn.close()


def get_huc_bbox_shapely_shape_obj(huc_type=None, huc_id=None):

    if huc_type not in ["huc_8", "huc_10", "huc_12"]:
        raise Exception("Only support huc_8, huc_10 and huc_12")
    if huc_type == "huc_8" and len(huc_id) != 8:
        raise Exception("Invalid huc_8 comid")
    elif huc_type == "huc_10" and len(huc_id) != 10:
        raise Exception("Invalid huc_10 comid")
    elif huc_type == "huc_12" and len(huc_id) != 12:
        raise Exception("Invalid huc_12 comid")

    huc_wkt = query_huc(huc_type=huc_type, huc_id=huc_id)

    shape_obj = shapely.wkt.loads(huc_wkt)

    return shape_obj, db_epsg


def query_huc(huc_type, huc_id):

    # sql_str_comids = 'select station_id from stream where {huc_type} = "{huc_id}"'.format(huc_type=huc_type.upper(),
    #                                                                                       huc_id=huc_id)
    sql_str_huc_wkt = 'select AsWKT(Shape) from {huc_type} where {huc_type} = "{huc_id}"'.format(huc_type=huc_type.upper(),
                                                                                             huc_id=huc_id)
    conn = None

    try:
        conn = db.connect(db_file)
        cursor = conn.execute(sql_str_huc_wkt)
        huc_wkt = cursor.fetchone()[0].encode('ascii', 'ignore')

        return huc_wkt
    except Exception as ex:
        raise ex
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":

    shp_path = "/home/drew/Desktop/state/utah_utm_nad83_zone_12.shp"
    #shp_path = "/home/drew/Desktop/state/state_local.shp"
    query_comids_and_grid_indices(query_type="shapefile", shp_path=shp_path)

    # geojson_str = '{"type":"Polygon",' \
    #               '"coordinates":[[[-109.436035, 43.011979],[-93.361976,42.599528],' \
    #               '[-92.768481,34.295595],[-111.418367,34.515505],[-109.436035, 43.011979]]]}'
    # input_epsg=4326
    # query_comids_and_grid_indices(query_type="geojson",
    #                                     geom_str=geojson_str,
    #                                     in_epsg=input_epsg)


    # input_wkt = "POLYGON((500000 4316776.583097936,500000 4427757.218624833,585360.4618433624 4428236.064519553,586592.6780279021 4317252.164517585,500000 4316776.583097936))"
    # input_epsg = "26912"
    # query_comids_and_grid_indices(query_type="wkt",
    #                                     geom_str=input_wkt,
    #                                     in_epsg=input_epsg)

    #query_comids_and_grid_indices(query_type="huc_12", huc_id="030902040700")
    #query_comids_and_grid_indices(query_type="huc_10", huc_id="1210030202")
    #query_comids_and_grid_indices(query_type="huc_8", huc_id="18090203")

    pass