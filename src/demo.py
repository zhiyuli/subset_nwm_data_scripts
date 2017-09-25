import os
import logging
import uuid
import datetime
from subset_nwm_netcdf.query import query_comids_and_grid_indices
from subset_nwm_netcdf.subset import start_subset_nwm_netcdf_job
from subset_nwm_netcdf.merge import start_merge_nwm_netcdf_job

job_id = str(uuid.uuid4())
logger = logging.getLogger()

# create root logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler('subset_nwm_netcdf_{job_id}.log'.format(job_id=job_id))
fh.setLevel(logging.DEBUG)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)

if __name__ == "__main__":

    all_start_dt = datetime.datetime.now()
    logger.info("-------------Process Started-------------------")
    logger.info(all_start_dt)
    try:
        # Path to geodatabase file (download from https://www.hydroshare.org/resource/23c05d3177654a9ab9dc9023d00d16ed/)
        # Windows
        #db_file_path = "F:/NWM/DB/nwm.sqlite"

        # Linux
        db_file_path = "/home/drew/Desktop/nwm.sqlite"

        ## Shapefile example: utah state polygon
        query_type = "shapefile"
        #shp_path = "./subset_nwm_netcdf/static/data/utah/utah_utm_nad83_zone_12.shp"
        # Shapefile example: TwoMileCreek watershed at Tuscaloosa, AL
        shp_path = "./subset_nwm_netcdf/static/data/TwoMileCreek/TwoMileCreek_poly.shp"
        geom_str = None
        in_epsg = None  # epsg is optional as lib will try reading epsg code from prj file.
                        # the run will fail if the prj file contains a custom projection string that does not have a epsg code,
        huc_id = None


        # # geojson example
        # query_type = "geojson"
        # shp_path = None
        # geom_str = '{ "type": "Polygon", "coordinates": [ [ [ -111.0, 39.000000000002537 ], [ -111.0, 40.000000000003041 ], [ -110.0, 40.000000000003048 ], [ -110.0, 39.000000000002551 ], [ -111.0, 39.000000000002537 ] ] ] }'
        # in_epsg = 4269  # NAD83; if no epsg is given then use EPSG:4326
        # huc_id = None

        # # wkt example
        # query_type = "wkt"
        # shp_path = None
        # geom_str = "POLYGON((500000 4316776.583097936,500000 4427757.218624833,585360.4618433624 4428236.064519553,586592.6780279021 4317252.164517585,500000 4316776.583097936))"
        # in_epsg = 26912  # NAD83 UTM zone 12; epsg is required
        # huc_id = None

        # # huc 12 example
        # # Bear River-Frontal Great Salt Lake watershed, UT
        # query_type = "huc_12"
        # shp_path = None
        # geom_str = None
        # in_epsg = None
        # huc_id = "160102040504"

        # # huc 10 example
        # # Upper Medina River watershed, TX
        # query_type = "huc_10"
        # shp_path = None
        # geom_str = None
        # in_epsg = None
        # huc_id = "1210030202"

        # # huc 8 example
        # # Southern Great Salt Lake Desert watershed, UT
        # query_type = "huc_8"
        # shp_path = None
        # geom_str = None
        # in_epsg = None
        # huc_id = "16020306"

        query_result_dict = query_comids_and_grid_indices(job_id=job_id,
                                                          db_file_path=db_file_path,
                                                          query_type=query_type,
                                                          shp_path=shp_path,
                                                          geom_str=geom_str,
                                                          in_epsg=in_epsg,
                                                          huc_id=huc_id)
        if query_result_dict is None:
            raise Exception("Failed to retrieve spatial query result")

        # Path to the root folder contains original NWM NetCDF files
        # Windows
        #netcdf_folder_path = "G:\\nwm_new_data"
        # Linux
        netcdf_folder_path = "/media/sf_nwm_new_data"

        # Path of output folder
        output_folder_path = "./temp"

        # shrink dimension size to cover subsetting domain only
        resize_dimension_grid = True
        resize_dimension_feature = True
        # merge resulting netcdfs
        merge_netcdfs = True
        # remove intermediate files
        cleanup = True
        include_AA_tm12 = False

        # list of simulation dates
        simulation_date_list = ["20170419"]

        # list of model file types
        #file_type_list = ["forecast", 'forcing']
        file_type_list = ["forcing"]

        # list of model configurations
        #model_configuration_list = ['analysis_assim', 'short_range', 'medium_range', 'long_range']
        model_configuration_list = ['analysis_assim']

        # list of model result data types
        #data_type_list = ['reservoir', 'channel', 'land', 'terrain']
        data_type_list = ['channel']

        # list of time stamps or model cycles
        # [1, 2, ...];  [] or None means all default time stamps
        time_stamp_list = []

        grid_land_dict = query_result_dict["grid_land"]
        grid_terrain_dict = query_result_dict["grid_terrain"]
        stream_comid_list = query_result_dict["stream"]["comids"]
        reservoir_comid_list = query_result_dict["reservoir"]["comids"]

        if "long_range" in model_configuration_list:
            model_configuration_list.remove("long_range")
            for i in range(1, 5):
                model_configuration_list.append("long_range_mem{0}".format(str(i)))

        output_netcdf_folder_path = os.path.join(output_folder_path, job_id)

        start_subset_nwm_netcdf_job(job_id=job_id,
                                    input_netcdf_folder_path=netcdf_folder_path,
                                    output_netcdf_folder_path=output_netcdf_folder_path,
                                    simulation_date_list=simulation_date_list,
                                    file_type_list=file_type_list,
                                    model_configuration_list=model_configuration_list,
                                    data_type_list=data_type_list,
                                    time_stamp_list=time_stamp_list,
                                    grid_land_dict=grid_land_dict,
                                    grid_terrain_dict=grid_terrain_dict,
                                    stream_comid_list=stream_comid_list,
                                    reservoir_comid_list=reservoir_comid_list,
                                    resize_dimension_grid=resize_dimension_grid,
                                    resize_dimension_feature=resize_dimension_feature,
                                    cleanup=cleanup,
                                    include_AA_tm12=include_AA_tm12)

        if merge_netcdfs:
            start_merge_nwm_netcdf_job(job_id=job_id,
                                       simulation_date_list=simulation_date_list,
                                       file_type_list=file_type_list,
                                       model_cfg_list=model_configuration_list,
                                       data_type_list=data_type_list,
                                       time_stamp_list=time_stamp_list,
                                       netcdf_folder_path=output_netcdf_folder_path,
                                       cleanup=cleanup)

    except Exception as ex:
        logger.exception(ex.message)
    finally:
        all_end_dt = datetime.datetime.now()
        logger.debug(all_end_dt)
        all_elapse_dt = all_end_dt - all_start_dt
        logger.info(all_elapse_dt)
        logger.info("--------------- Process Ended {0}----------------".format(job_id))
