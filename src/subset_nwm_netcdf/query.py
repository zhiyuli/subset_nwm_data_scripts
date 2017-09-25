import os
import json
import exceptions
import logging
import datetime
import copy
# from functools import partial

import netCDF4
import numpy
import fiona
import shapely.wkt
import shapely.geometry
import shapely.ops
# import pyproj
from osgeo import ogr
from osgeo import osr

pyspatialite_load = False
try:
    import pyspatialite.dbapi2 as db
    pyspatialite_load = True
except ImportError:
    try:
        import pysqlite2.dbapi2 as db  # mod_spatialite extension should be installed
    except ImportError:
        raise Exception("Can not load required lib pyspatialite or pysqlite2.")

logger = logging.getLogger('subset_nwm_netcdf')

default_xy_templates_path = os.path.join(os.path.dirname(__file__), "static/netcdf_templates/v1.1/xy_nc/")


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
    :param query_type: "shapefile", "wkt", "geojson", "huc_12", "huc_10", "huc_8", ("stream": not supported yet)
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

    if job_id is None:
        job_id = datetime.datetime.now().strftime("_%Y_%m_%d_")
    logger.info("---------------Performing Spatial Query {0}----------------".format(job_id))
    sq_start_dt = datetime.datetime.now()
    logger.info(sq_start_dt)

    logger.info("db_file_path: {0}".format(str(db_file_path)))
    logger.info("db_epsg_code: {0}".format(str(db_epsg_code)))
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
        query_land_grid = True
        query_terrain_grid = True
        query_reservoir = True
        if query_type_lower in ["shapefile", "geojson", "wkt"]:

            wkt_polygon, in_epsg_checked = _get_first_polygon_exterior_wkt(db_file=db_file_path,
                                                                           query_type_lower=query_type_lower,
                                                                           in_epsg=in_epsg,
                                                                           shp_path=shp_path,
                                                                           geom_str=geom_str)
        elif "huc" in query_type_lower:

            wkt_polygon, in_epsg_checked = _get_huc_polygon_wkt(db_file=db_file_path,
                                                                db_epsg=db_epsg_code,
                                                                huc_type=query_type_lower,
                                                                huc_id=huc_id)
        elif "stream" == query_type_lower:
            raise exceptions.NotImplementedError()

        data = _perform_spatial_query(db_file=db_file_path,
                                      db_epsg=db_epsg_code,
                                      query_window_wkt=wkt_polygon,
                                      input_epsg=in_epsg_checked,
                                      query_stream=query_stream,
                                      query_land_grid=query_land_grid,
                                      query_terrain_grid=query_terrain_grid,
                                      query_reservoir=query_reservoir)

        if data["status"] == "success":
            if "grid_land" in data:
                logger.info("grid_land: {0}".format(str(data["grid_land"])))
                dim_x_len = data["grid_land"]['maxX'] - data["grid_land"]['minX'] + 1
                dim_y_len = data["grid_land"]['maxY'] - data["grid_land"]['minY'] + 1
                logger.info("{x_len} * {y_len} = {cells}".format(x_len=str(dim_x_len), y_len=str(dim_y_len),
                                                                 cells=str(dim_x_len * dim_y_len)))
            if "grid_terrain" in data:
                logger.info("grid_terrain: {0}".format(str(data["grid_terrain"])))
                dim_x_len = data["grid_terrain"]['maxX'] - data["grid_terrain"]['minX'] + 1
                dim_y_len = data["grid_terrain"]['maxY'] - data["grid_terrain"]['minY'] + 1
                logger.info("{x_len} * {y_len} = {cells}".format(x_len=str(dim_x_len), y_len=str(dim_y_len),
                                                                 cells=str(dim_x_len * dim_y_len)))

            logger.info("stream count: {0}".format(str(data["stream"]["count"])))
            logger.info("reservoir count: {0}".format(str(data["reservoir"]["count"])))
        else:
            raise Exception()

        return data
    except Exception as ex:
        logger.error("Spatial Query Failed")
        logger.exception("{0}: {1}".format(str(type(ex)), ex.message))
    finally:
        sq_end_dt = datetime.datetime.now()
        logger.debug(sq_end_dt)
        sq_elapse_dt = sq_end_dt - sq_start_dt
        logger.info("Done in {0}".format(sq_elapse_dt))
        logger.info("--------------- Spatial Query Done {job_id}----------------".format(job_id=job_id))


# def _get_shapely_shape_obj(db_file=None, query_type_lower=None, in_epsg=None, shp_path=None, geom_str=None):
#
#     if query_type_lower is None:
#         raise Exception("Parameter 'query_type_lower' is not given")
#
#     if in_epsg is not None and not _check_supported_epsg(epsg=in_epsg, db_file=db_file):
#         raise Exception("A invalid/unsupported epsg code is given")
#
#     in_epsg_checked = in_epsg
#
#     if query_type_lower == "shapefile":
#         if shp_path is None or not os.path.exists(shp_path):
#             raise Exception("Shp path is invalid")
#         shp_obj = fiona.open(shp_path)
#
#         if in_epsg_checked is None:
#             # check shapefile prj for epsg
#             logger.info("User did not declare epsg code for this shapefile. Trying to extract epsg code from shapefile prj file....")
#             if "init" in shp_obj.crs:
#                 epsg = shp_obj.crs["init"].split(":")[1]
#                 if _check_supported_epsg(epsg=epsg, db_file=db_file):
#                     in_epsg_checked = int(epsg)
#                     logger.info("epsg: {0}".format(str(in_epsg_checked)))
#                 else:
#                     raise Exception("Shapefile has unsupported projection/epsg code ")
#             else:
#                 raise Exception("Shapefile has no or invalid projection/epsg code")
#
#         # parse shp geom
#         # check geometry type
#         geom_type = shp_obj.schema['geometry'].lower()
#         if geom_type not in ["polygon", "multipolygon"]:
#             raise Exception("Shapefile must be type of Polygon or MultiPolygon")
#
#         first_feature_obj = next(shp_obj)
#         shape_obj = shapely.geometry.shape(first_feature_obj["geometry"])
#
#         # geojson = shapely.geometry.mapping(shape_obj)
#         # wkt_str = shape_obj.wkt
#
#     elif query_type_lower == "geojson":
#
#         if in_epsg_checked is None:
#             logger.warning("No epsg code is given for geojson. Assume EPSG:4326")
#         geojson = json.loads(geom_str)
#         shape_obj = shapely.geometry.asShape(geojson)
#
#     elif query_type_lower == "wkt":
#         if in_epsg_checked is None:
#             raise Exception("No epsg code is given")
#         shape_obj = shapely.wkt.loads(geom_str)
#     else:
#         raise Exception("Unknown query_type: {0}".format(query_type_lower))
#
#     return shape_obj, in_epsg_checked


def _get_first_polygon_exterior_wkt(db_file=None, query_type_lower=None, in_epsg=None, shp_path=None, geom_str=None):

    well_known_epsg = 4269
    prj_path = None

    if query_type_lower is None:
        raise Exception("Parameter 'query_type_lower' is not given")

    if query_type_lower == "shapefile":
        if shp_path is None or not os.path.exists(shp_path):
            raise Exception("Shp path is invalid")
        prj_path = shp_path.replace(".shp", ".prj")
        if not os.path.exists(prj_path):
            raise Exception("Can not find projection file (*.prj).")

        shp_obj_fiona = fiona.open(shp_path)

        # check shapefile prj for epsg code
        if "init" in shp_obj_fiona.crs:
            in_epsg = shp_obj_fiona.crs["init"].split(":")[1]
        else:
            in_epsg = -9999
        logger.info("Trying to extract epsg code from shapefile prj file: {0}".format(in_epsg))

        # parse shp geom
        # check geometry type
        geom_type = shp_obj_fiona.schema['geometry'].lower()
        if geom_type not in ["polygon", "multipolygon"]:
            raise Exception("Shapefile must be type of Polygon or MultiPolygon")

        first_feature_obj = next(shp_obj_fiona)
        shape_obj = shapely.geometry.shape(first_feature_obj["geometry"])

    elif query_type_lower == "geojson":

        if in_epsg is None:
            raise Exception("No epsg code is given for geojson")
        geojson = json.loads(geom_str)
        shape_obj = shapely.geometry.asShape(geojson)

    elif query_type_lower == "wkt":
        if in_epsg is None:
            raise Exception("No epsg code is given for WKT")
        shape_obj = shapely.wkt.loads(geom_str)
    else:
        raise Exception("Unknown query_type: {0}".format(query_type_lower))

    # convert 3D geom to 2D
    if shape_obj.has_z:
        wkt2D = shape_obj.wkt
        shape_obj = shapely.wkt.loads(wkt2D)

    # check shaps_obj is "polygon" or "multipolygon"
    if shape_obj.geom_type.lower() == "multipolygon":
        polygon_exterior_linearring = shape_obj[0].exterior
    elif shape_obj.geom_type.lower() == "polygon":
        polygon_exterior_linearring = shape_obj.exterior
    else:
        raise Exception("Input Geometry is not Polygon")

    polygon_exterior_linearring_shape_obj = shapely.geometry.Polygon(polygon_exterior_linearring)

    # check in_epsg is supported in SQLite db
    if _check_supported_epsg(epsg=in_epsg, db_file=db_file):
        in_epsg_checked = in_epsg
        wkt_polygon = polygon_exterior_linearring_shape_obj.wkt
    else:
        # in_epsg code is not supported by SQLite db, need to re-project it into a well-known projection: 4326
        if query_type_lower == "geojson" or query_type_lower == "wkt":
            in_proj_type = "epsg"
            in_proj_value = in_epsg

        elif query_type_lower == "shapefile":
            with open(prj_path, 'r') as content_file:
                prj_str_raw = content_file.read()
                prj_str = prj_str_raw.replace('\n', '')
                in_proj_type = "esri"
                in_proj_value = prj_str
        # use GDAL for coordinate re-projection
        wkt_reprojected = reproject_wkt_gdal(in_proj_type=in_proj_type,
                                             in_proj_value=in_proj_value,
                                             out_proj_type="epsg",
                                             out_proj_value=well_known_epsg,
                                             in_geom_wkt=polygon_exterior_linearring_shape_obj.wkt)

        in_epsg_checked = well_known_epsg
        wkt_polygon = wkt_reprojected

    return wkt_polygon, in_epsg_checked


def reproject_wkt_gdal(in_proj_type,
                       in_proj_value,
                       out_proj_type,
                       out_proj_value,
                       in_geom_wkt):

    try:
        if 'GDAL_DATA' not in os.environ:
            raise Exception("Environment variable 'GDAL_DATA' not found!")

        source = osr.SpatialReference()
        if in_proj_type.lower() == "epsg":
            source.ImportFromEPSG(int(in_proj_value))
        elif in_proj_type.lower() == "proj4":
            source.ImportFromProj4(in_proj_value)
        elif in_proj_type.lower() == "esri":
            source.ImportFromESRI([in_proj_value])
        else:
            raise Exception("unsupported projection type: " + out_proj_type)

        target = osr.SpatialReference()
        if out_proj_type.lower() == "epsg":
            target.ImportFromEPSG(out_proj_value)
        elif out_proj_type.lower() == "proj4":
            target.ImportFromProj4(out_proj_value)
        elif out_proj_type.lower() == "esri":
            target.ImportFromESRI([out_proj_value])
        else:
            raise Exception("unsupported projection type: " + out_proj_type)

        transform = osr.CoordinateTransformation(source, target)

        geom_gdal = ogr.CreateGeometryFromWkt(in_geom_wkt)
        geom_gdal.Transform(transform)

        return geom_gdal.ExportToWkt()

    except Exception as ex:
        logger.error("in_proj_type: {0}".format(in_proj_type))
        logger.error("in_proj_value: {0}".format(in_proj_value))
        logger.error("out_proj_type: {0}".format(out_proj_type))
        logger.error("out_proj_value: {0}".format(out_proj_value))
        logger.error(str(type(ex)) + " " + ex.message)
        raise ex


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
                           query_land_grid=True,
                           query_terrain_grid=True,
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
        if not pyspatialite_load:
            conn.enable_load_extension(True)
            conn.execute("SELECT load_extension('mod_spatialite')")
        geometry_field_name = 'Shape'

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

        if query_terrain_grid:
            xy_netcdf_path = os.path.join(default_xy_templates_path, "xy_terrain.nc")
            data['grid_terrain'] = _query_grid_indices_xy_nc(wkt_str=query_window_wkt,
                                                             wkt_epsg=input_epsg,
                                                             xy_netcdf_path=xy_netcdf_path
                                                             )
        if query_land_grid:
            xy_netcdf_path = os.path.join(default_xy_templates_path, "xy_land.nc")
            data['grid_land'] = _query_grid_indices_xy_nc(wkt_str=query_window_wkt,
                                                          wkt_epsg=input_epsg,
                                                          xy_netcdf_path=xy_netcdf_path
                                                          )
        return data

    except Exception as ex:
        data["status"] = "error"
        logger.exception(ex.message)
    finally:
        if conn is not None:
            conn.close()


def _query_grid_indices_xy_nc(wkt_str=None,
                              wkt_epsg=None,
                              xy_netcdf_path=None,
                              ):

    # 1) re-project query geometry into the proj same as forcing, land and terrain files
    # 2) get the bounding box
    # 3) get indices for bounding box corner points (inspired by: http://kbkb-wx-python.blogspot.com/2016/08/find-nearest-latitude-and-longitude.html)

    # shape_obj = shapely.wkt.loads(wkt_str)
    # in_pyproj_obj = pyproj.Proj(init='epsg:{wkt_epsg}'.format(wkt_epsg=wkt_epsg))

    ## custom proj.4 projection string used by all forcing, land and terrain files (NWM v1.1)
    # forcing_proj4 = '+proj=lcc +lat_1=30 +lat_2=60 +lat_0=40 +lon_0=-97 +x_0=0 +y_0=0 +a=6370000 +b=6370000 +units=m +no_defs'
    # forcing_pyproj_obj = pyproj.Proj(forcing_proj4)


    # shape_obj_reprojected = _project_shapely_geom(in_geom_obj=shape_obj,
    #                                               in_proj_type="pyproj",
    #                                               in_proj_value=in_pyproj_obj,
    #                                               out_proj_type="pyproj",
    #                                               out_proj_value=forcing_pyproj_obj)

    forcing_proj4 = '+proj=lcc +lat_1=30 +lat_2=60 +lat_0=40 +lon_0=-97 +x_0=0 +y_0=0 +a=6370000 +b=6370000 +units=m +no_defs'
    wkt_str_reprojected = reproject_wkt_gdal("epsg",
                                             wkt_epsg,
                                             "proj4",
                                             forcing_proj4,
                                             wkt_str)
    shape_obj_reprojected = shapely.wkt.loads(wkt_str_reprojected)

    minX = shape_obj_reprojected.bounds[0]
    maxX = shape_obj_reprojected.bounds[2]
    minY = shape_obj_reprojected.bounds[1]
    maxY = shape_obj_reprojected.bounds[3]

    offset_idx_dict, _, _ = \
        _find_closest_value_from_1d_netcdf(netcdf_path=xy_netcdf_path,
                                           search_value_dict={"x": [minX, maxX],
                                                              "y": [minY, maxY]}
                                           )
    return {"minX": offset_idx_dict['x'][0],
            "maxX": offset_idx_dict['x'][1],
            "minY": offset_idx_dict['y'][0],
            "maxY": offset_idx_dict['y'][1]}


def _find_closest_value_from_1d_netcdf(netcdf_path=None, search_value_dict=None, idx_offset=0):

    # open a netcdf file
    # find the closest values for a list of user-provided values of specific variable names defined in search_value_dict
    # add idx_offset to the Min and Max indices

    closest_idx_dict = copy.deepcopy(search_value_dict)
    closest_value_dict = copy.deepcopy(search_value_dict)
    offset_value_dict = copy.deepcopy(search_value_dict)

    var_length_dict = {}

    with netCDF4.Dataset(netcdf_path, mode='r') as in_nc:
        for var_name, search_value_list in search_value_dict.iteritems():
            var_length_dict[var_name] = len(in_nc.variables[var_name][:])
            var_value_array = in_nc.variables[var_name][:]
            for i in range(len(search_value_list)):
                search_value = search_value_list[i]
                diff_array = numpy.abs(search_value - var_value_array)
                closest_idx = numpy.argmin(diff_array)
                closest_idx_dict[var_name][i] = closest_idx

                closest_value = var_value_array[closest_idx]
                closest_value_dict[var_name][i] = closest_value

    # add offset to idx
    for var_name, idx_list in closest_idx_dict.iteritems():
        idx_min = min(idx_list)
        idx_max = max(idx_list)
        if idx_min > 0:
            idx_min = idx_min - idx_offset
        if idx_max < var_length_dict[var_name]:
            idx_max = idx_max + idx_offset
        offset_value_dict[var_name] = [idx_min, idx_max]

    return offset_value_dict, closest_idx_dict, closest_value_dict


# def _project_shapely_geom(in_geom_obj=None,
#                           in_proj_type=None,
#                           in_proj_value=None,
#                           out_proj_type=None,
#                           out_proj_value=None):
#
#     # re-project a shapely geometry object
#     # the in and out projection can be defined by epsg code, proj4 string or pyproj obj
#
#     # USE pycrs lib for conversion between different types of projection string
#     # Especially useful when dealing with shapefile with prj
#     # Covert any type of projection string to proj.4 string
#     # fromcrs = pycrs.parser.from_unknown_text(proj_str)
#     # fromcrs_proj4 = fromcrs.to_proj4()
#
#     if in_proj_type.lower() == "epsg":
#         in_pyproj_obj = pyproj.Proj(init='epsg:{wkt_epsg}'.format(wkt_epsg=in_proj_value))
#     elif in_proj_type.lower() == "proj4":
#          in_pyproj_obj = pyproj.Proj(in_proj_value)
#     elif in_proj_type.lower() == "pyproj":
#          in_pyproj_obj = in_proj_value
#
#     if out_proj_type.lower() == "epsg":
#         out_pyproj_obj = pyproj.Proj(init='epsg:{wkt_epsg}'.format(wkt_epsg=out_proj_value))
#     elif out_proj_type.lower() == "proj4":
#         out_pyproj_obj = pyproj.Proj(out_proj_value)
#     elif out_proj_type.lower() == "pyproj":
#          out_pyproj_obj = out_proj_value
#
#     project = partial(
#         pyproj.transform,
#         in_pyproj_obj,
#         out_pyproj_obj)
#     out_geom_obj = shapely.ops.transform(project, in_geom_obj)
#
#     return out_geom_obj

# # Deprecated
# def _query_grid_indices_tif(wkt_str=None,
#                         wkt_epsg=None,
#                         tif_file_path=None,
#                         tif_epsg=4269
#                         ):
#     from osgeo import gdal, ogr, osr
#
#     geom_obj = ogr.CreateGeometryFromWkt(wkt_str)
#
#     source = osr.SpatialReference()
#     source.ImportFromEPSG(wkt_epsg)
#
#     ds_tiff = gdal.Open(tif_file_path)
#     prj_tiff = ds_tiff.GetProjection()
#     target = osr.SpatialReference(wkt=prj_tiff)
#
#     # target = osr.SpatialReference()
#     # target.ImportFromEPSG(tif_epsg)
#     #prj4_str = "+proj=lcc +a=6370000.0 +f=0.0 +pm=0.0  +x_0=0.0 +y_0=0.0 +lon_0=-97.0 +lat_1=30.0 +lat_2=60.0 +lat_0=40.0000076294 +units=m +axis=enu +no_defs"
#     #target.ImportFromProj4(prj4_str)
#     #target.ImportFromESRI('./subset_nwm_netcdf/tests/Sphere_Lambert_Conformal_Conic.prj')
#     transform = osr.CoordinateTransformation(source, target)
#     geom_obj.Transform(transform)
#
#     # ds = gdal.Warp("./{0}".format(os.path.basename(tif_file_path)),
#     #                tif_file_path,
#     #                format='GTiff',
#     #                cutlineDSName=geom_obj.ExportToJson(),
#     #                cropToCutline=True,
#     #                # transformerOptions=['DST_SRS=26912'],
#     #                dstNodata=65535,
#     #                warpOptions=['CUTLINE_ALL_TOUCHED=TRUE'])
#
#     # gdal.Warp() implemented in GDAL 2.1 and later
#     ds = gdal.Warp("",
#                    tif_file_path,
#                    format='MEM',
#                    cutlineDSName=geom_obj.ExportToJson(),
#                    cropToCutline=True,
#                    dstNodata=65535,
#                    warpOptions=['CUTLINE_ALL_TOUCHED=TRUE'])
#
#     x_band_stats = ds.GetRasterBand(1).GetStatistics(False, True)
#     x_min = int(x_band_stats[0])
#     x_max = int(x_band_stats[1])
#     y_band_stats = ds.GetRasterBand(2).GetStatistics(False, True)
#     y_min = int(y_band_stats[0])
#     y_max = int(y_band_stats[1])
#
#     return {"minX": x_min, "maxX": x_max, "minY": y_min, "maxY": y_max}


def _get_huc_polygon_wkt(db_file=None, db_epsg=None, huc_type=None, huc_id=None):

    if huc_type not in ["huc_8", "huc_10", "huc_12"]:
        raise Exception("Only support huc_8, huc_10 and huc_12")
    if huc_type == "huc_8" and len(huc_id) != 8:
        raise Exception("Invalid huc_8 comid")
    elif huc_type == "huc_10" and len(huc_id) != 10:
        raise Exception("Invalid huc_10 comid")
    elif huc_type == "huc_12" and len(huc_id) != 12:
        raise Exception("Invalid huc_12 comid")

    huc_wkt = _query_huc(db_file=db_file, huc_type=huc_type, huc_id=huc_id)

    #shape_obj = shapely.wkt.loads(huc_wkt)

    return huc_wkt, db_epsg


def _query_huc(db_file=None, huc_type=None, huc_id=None):

    # sql_str_comids = 'select station_id from stream where {huc_type} = "{huc_id}"'.format(huc_type=huc_type.upper(),
    #                                                                                       huc_id=huc_id)
    sql_str_huc_wkt = 'select AsWKT(Shape) from {huc_type} where {huc_type} = "{huc_id}"'.format(huc_type=huc_type.upper(),
                                                                                                 huc_id=huc_id)
    conn = None

    try:
        conn = db.connect(db_file)
        if not pyspatialite_load:
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
