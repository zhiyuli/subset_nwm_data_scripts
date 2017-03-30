import logging
import uuid
import datetime
from subset_nwm_netcdf.query import query_comids_and_grid_indices
from subset_nwm_netcdf.subsetting import start_subset_nwm_netcdf_job

job_id = str(uuid.uuid4())

# create logger with 'subset_netcdf'
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
        # Windows
        db_file_path = "F:/NWM/DB/nwm.sqlite"
        # Linux
        #db_file_path = "/home/drew/Desktop/nwm.sqlite"


        # # Shapefile utah
        # query_type = "shapefile"
        # shp_path = "./statics/data/utah/utah_utm_nad83_zone_12.shp"
        # geom_str = None
        # in_epsg = None
        # huc_id = None

        # # geojson
        # query_type = "geojson"
        # shp_path = None
        # geom_str = '{"type":"Polygon",' \
        #            '"coordinates":[[[-109.436035, 43.011979],[-93.361976,42.599528],' \
        #            '[-92.768481,34.295595],[-111.418367,34.515505],[-109.436035, 43.011979]]]}'
        # in_epsg = 4326
        # huc_id = None

        # # wkt
        # query_type = "wkt"
        # shp_path = None
        # geom_str = "POLYGON((500000 4316776.583097936,500000 4427757.218624833,585360.4618433624 4428236.064519553,586592.6780279021 4317252.164517585,500000 4316776.583097936))"
        # in_epsg = 26912
        # huc_id = None

        # huc 12
        query_type = "huc_12"
        shp_path = None
        geom_str = None
        in_epsg = None
        huc_id = "160102040504"

        # # huc 10
        # query_type = "huc_10"
        # shp_path = None
        # geom_str = None
        # in_epsg = None
        # huc_id = "1210030202"

        # # huc 8
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

        # Windows
        netcdf_folder_path = "G:\\nwm_new_data"
        # Linux
        #netcdf_folder_path = "/media/sf_nwm_new_data"

        output_folder_path = "./temp"
        merge_netcdfs = True
        cleanup = True
        simulation_date_list = ["20170327"]
        #data_type_list = ["forecast", 'forcing']
        data_type_list = ["forecast"]
        #model_type_list = ['analysis_assim', 'short_range', 'medium_range', 'long_range']
        model_type_list = ['analysis_assim']
        #file_type_list = ['channel', 'reservoir', 'land']
        file_type_list = ['land']
        time_stamp_list = []  # ["1, 2, ...];  [] or None means all default time stamps

        grid_dict = query_result_dict["grid_land"]
        stream_comid_list = query_result_dict["stream"]["comids"]
        reservoir_comid_list = query_result_dict["reservoir"]["comids"]

        start_subset_nwm_netcdf_job(job_id=job_id,
                                    netcdf_folder_path=netcdf_folder_path,
                                    output_folder_path=output_folder_path,
                                    simulation_date_list=simulation_date_list,
                                    data_type_list=data_type_list,
                                    model_type_list=model_type_list,
                                    file_type_list=file_type_list,
                                    time_stamp_list=time_stamp_list,
                                    grid_dict=grid_dict,
                                    stream_comid_list=stream_comid_list,
                                    reservoir_comid_list=reservoir_comid_list,
                                    merge_netcdfs=merge_netcdfs,
                                    cleanup=cleanup)

    except Exception as ex:
        logger.exception(ex.message)
    finally:
        all_end_dt = datetime.datetime.now()
        logger.debug(all_end_dt)
        all_elapse_dt = all_end_dt - all_start_dt
        logger.info(all_elapse_dt)
        logger.info("--------------- Process Ended {0}----------------".format(job_id))
