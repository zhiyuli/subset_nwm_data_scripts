import os
import json
import exceptions
import logging
import datetime

import fiona
import shapely.wkt
import shapely.geometry
# import pyspatialite.dbapi2 as db
import pysqlite2.dbapi2 as db


logger = logging.getLogger('subset_nwm_netcdf')


def query_comids_and_grid_indices(job_id=None,
                                  db_file_path=None,
                                  db_epsg_code=4269,
                                  query_type="shapefile",
                                  shp_path=None,
                                  geom_str=None,
                                  in_epsg=None,
                                  huc_id=None,
                                  stream_list=[]):

    '''
    Query stream comids, reservoir comids and grid cells using a polygon
    :param db_file_path: full path to NWM spatialite geodatabase file
    :param db_epsg_code: the epsg code of NWM spatialite geodatabase (default 4269)
    :param job_id: a job identifier
    :param query_type: "shapefile", "wkt", "geojson", "huc_12", "huc_10", "huc_8", "stream"
    :param shp_path: full path to *.shp file
    :param geom_str: wkt or geojson string
    :param in_epsg: epsg code of geom_str or shapefile
    :param huc_id: comid of a huc watershed, of which the geometry will used as querying polygon
    :param stream_list: not implemented yet, keep unchanged
    :return: a dict contains query results:
                        { "grid_land" {"minx": 11, "maxX": 22, "minY": 33, "maxY": 44},
                           "stream": {"count": 500, "comids": []},
                           "reservoir": {"count": 500, "comids": []}
                        }
    '''

    logger.info("---------------Performing Spatial Query {0}----------------".format(job_id))
    sq_start_dt = datetime.datetime.now()
    logger.debug(sq_start_dt)

    logger.info("query_type: {0}".format(str(query_type)))
    logger.info("shp_path: {0}".format(str(shp_path)))
    logger.info("geom_str: {0}".format(str(geom_str)))
    logger.info("in_epsg: {0}".format(str(in_epsg)))
    logger.info("huc_id: {0}".format(str(huc_id)))
    logger.info("stream_list: {0}".format(str(stream_list)))

    try:
        in_epsg_checked = None
        query_type_lower = query_type.lower()
        query_stream = True
        query_grid = True
        query_reservoir = True
        if query_type_lower in ["shapefile", "geojson", "wkt"]:

            shape_obj, in_epsg_checked = _get_shapely_shape_obj(db_file=db_file_path, query_type_lower=query_type_lower,
                                                                in_epsg=in_epsg, shp_path=shp_path, geom_str=geom_str)
        elif "huc" in query_type_lower:

            shape_obj, in_epsg_checked = _get_huc_bbox_shapely_shape_obj(db_file=db_file_path,
                                                                         db_epsg=db_epsg_code,
                                                                         huc_type=query_type_lower,
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
        data = _perform_spatial_query(db_file=db_file_path,
                                      db_epsg=db_epsg_code,
                                      query_window_wkt=polygon_query_window.wkt,
                                      input_epsg=in_epsg_checked,
                                      query_stream=query_stream,
                                      query_grid=query_grid,
                                      query_reservoir=query_reservoir)

        logger.info("grid: {0}".format(str(data["grid_land"])))
        dim_x_len = data["grid_land"]['maxX'] - data["grid_land"]['minX'] + 1
        dim_y_len = data["grid_land"]['maxY'] - data["grid_land"]['minY'] + 1
        logger.info("{x_len} * {y_len} = {cells}".format(x_len=str(dim_x_len), y_len=str(dim_y_len),
                                                         cells=str(dim_x_len * dim_y_len)))
        logger.info("stream count: {0}".format(str(data["stream"]["count"])))
        logger.info("reservoir count: {0}".format(str(data["reservoir"]["count"])))

        return data
    except Exception as ex:
        logger.error("Spatial Query Failed")
        logger.exception(ex.message)
    finally:
        sq_end_dt = datetime.datetime.now()
        logger.debug(sq_end_dt)
        sq_elapse_dt = sq_end_dt - sq_start_dt
        logger.info("Done in {0}".format(sq_elapse_dt))
        logger.info("--------------- Spatial Query Done----------------")


def _get_shapely_shape_obj(db_file=None, query_type_lower=None, in_epsg=None, shp_path=None, geom_str=None):

    if query_type_lower is None:
        raise Exception("Parameter 'query_type_lower' is not given")

    if in_epsg is not None and not _check_supported_epsg(epsg=in_epsg, db_file=db_file):
        raise Exception("A invalid/unsupported epsg code is given")

    in_epsg_checked = in_epsg

    if query_type_lower == "shapefile":
        if shp_path is None or not os.path.exists(shp_path):
            raise Exception("Shp path is invalid")
        shp_obj = fiona.open(shp_path)

        if in_epsg_checked is None:
            # check shapefile prj for epsg
            logger.info("User did not declare epsg code for this shapefile. Trying to extract epsg code from shapefile prj file....")
            if "init" in shp_obj.crs:
                epsg = shp_obj.crs["init"].split(":")[1]
                if _check_supported_epsg(epsg=epsg, db_file=db_file):
                    in_epsg_checked = int(epsg)
                    logger.info("epsg: {0}".format(str(in_epsg_checked)))
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


def _check_supported_epsg(epsg=None, db_file=None):

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
        logger.exception(ex.message)
        raise ex
    finally:
        if conn is not None:
            conn.close()


def _perform_spatial_query(db_file=None,
                          db_epsg=None,
                          query_window_wkt=None,
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
        conn.enable_load_extension(True)
        conn.execute("SELECT load_extension('mod_spatialite')")
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
            query_table_name = 'reservoir'
            query_string = 'COMID'

            sql_reservoir = sql_template.format(query_table_name=query_table_name,
                                                query_string=query_string,
                                                geometry_field_name=geometry_field_name,
                                                input_geom=input_geom)

            cursor = conn.execute(sql_reservoir)
            reservoir_comids = [item[0] for item in cursor.fetchall()]
            data['reservoir'] = {"count": len(reservoir_comids), "comids": reservoir_comids}

            conn.close()

        return data

    except Exception as ex:
        data["status"] = "error"
        raise ex
    finally:
        if conn is not None:
            conn.close()


def _get_huc_bbox_shapely_shape_obj(db_file=None, db_epsg=None, huc_type=None, huc_id=None):

    if huc_type not in ["huc_8", "huc_10", "huc_12"]:
        raise Exception("Only support huc_8, huc_10 and huc_12")
    if huc_type == "huc_8" and len(huc_id) != 8:
        raise Exception("Invalid huc_8 comid")
    elif huc_type == "huc_10" and len(huc_id) != 10:
        raise Exception("Invalid huc_10 comid")
    elif huc_type == "huc_12" and len(huc_id) != 12:
        raise Exception("Invalid huc_12 comid")

    huc_wkt = _query_huc(db_file=db_file, huc_type=huc_type, huc_id=huc_id)

    shape_obj = shapely.wkt.loads(huc_wkt)

    return shape_obj, db_epsg


def _query_huc(db_file=None, huc_type=None, huc_id=None):

    # sql_str_comids = 'select station_id from stream where {huc_type} = "{huc_id}"'.format(huc_type=huc_type.upper(),
    #                                                                                       huc_id=huc_id)
    sql_str_huc_wkt = 'select AsWKT(Shape) from {huc_type} where {huc_type} = "{huc_id}"'.format(huc_type=huc_type.upper(),
                                                                                                 huc_id=huc_id)
    conn = None

    try:
        conn = db.connect(db_file)
        conn.enable_load_extension(True)
        conn.execute("SELECT load_extension('mod_spatialite')")
        cursor = conn.execute(sql_str_huc_wkt)
        huc_wkt = cursor.fetchone()[0].encode('ascii', 'ignore')

        return huc_wkt
    except Exception as ex:
        logger.exception(ex.message)
        raise ex
    finally:
        if conn is not None:
            conn.close()
