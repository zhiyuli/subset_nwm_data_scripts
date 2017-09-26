import os
import subprocess
import datetime
import re
import copy
import logging
import glob

logger = logging.getLogger('subset_nwm_netcdf')


def start_merge_nwm_netcdf_job(job_id=None,
                               simulation_date_list=None,
                               file_type_list=None,
                               model_cfg_list=None,
                               data_type_list=None,
                               time_stamp_list=None,
                               netcdf_folder_path=None,
                               cleanup=True):
    try:
        if job_id is None:
            job_id = datetime.datetime.now().strftime("_%Y_%m_%d_")
        logger.info("---------------------Start Merging {job_id}-----------------------------".format(job_id=job_id))
        merge_start_dt = datetime.datetime.now()
        logger.debug(merge_start_dt)

        _merge_nwm_netcdf(simulation_date_list=simulation_date_list,
                          file_type_list=file_type_list,
                          model_cfg_list=model_cfg_list,
                          data_type_list=data_type_list,
                          time_stamp_list=time_stamp_list,
                          input_base_path=netcdf_folder_path,
                          output_base_path=netcdf_folder_path,
                          cleanup=cleanup)

        merge_end_dt = datetime.datetime.now()
        logger.debug(merge_end_dt)
        merge_elapse_dt = merge_end_dt - merge_start_dt
        logger.info("Merge Done in {0}".format(merge_elapse_dt))
        logger.info("---------------------Merge Done {job_id}-----------------------------".format(job_id=job_id))
    except Exception as ex:
        logger.exception(str(type(ex)) + ex.message)
    pass


def _merge_nwm_netcdf(simulation_date_list=None,
                     file_type_list=None,
                     model_cfg_list=None,
                     data_type_list=None,
                     time_stamp_list=None,
                     input_base_path=None,
                     output_base_path=None,
                     cleanup=True):

    if output_base_path is None:
        output_base_path = input_base_path

    for simulation_date in simulation_date_list:
        simulation_folder_name = "nwm.{simulation_date}".format(simulation_date=simulation_date)
        logger.info("Merging {0}".format(simulation_folder_name))
        input_simulation_folder_path = os.path.join(input_base_path, simulation_folder_name)
        output_simulation_folder_path = os.path.join(output_base_path, simulation_folder_name)
        for file_type in file_type_list:
            for model_cfg in model_cfg_list:
                if file_type == "forcing":
                    model_configuration_folder_name = file_type + "_" + model_cfg
                    input_folder_path = os.path.join(input_simulation_folder_path, model_configuration_folder_name)
                    output_folder_path = os.path.join(output_simulation_folder_path, model_configuration_folder_name)
                    if "analysis_assim" == model_cfg:
                        fn_template = "nwm.t{HH}z.analysis_assim.forcing.tm{XXX}.conus.nc"
                        HH_re_list = ["\d\d"]
                        HH_merged_list = ["ALL"]
                        XXX_re_list = ["00", "01", "02"]
                        XXX_merged_list = ["00", "01", "02"]
                    elif "short_range" == model_cfg:
                        fn_template = "nwm.t{HH}z.short_range.forcing.f{XXX}.conus.nc"
                        HH_re_list = [str(i).zfill(2) for i in range(0, 24)]
                        HH_merged_list = HH_re_list
                        XXX_re_list = ["\d\d\d"]
                        XXX_merged_list = ["ALL"]
                    elif "medium_range" == model_cfg:
                        fn_template = "nwm.t{HH}z.medium_range.forcing.f{XXX}.conus.nc"
                        HH_re_list = [str(i).zfill(2) for i in range(0, 19, 6)]
                        HH_merged_list = HH_re_list
                        XXX_re_list = ["\d\d\d"]
                        XXX_merged_list = ["ALL"]
                    log_str = "Merging {file_type}-{model_cfg}".format(file_type=file_type,
                                                                       model_cfg=model_cfg)
                    logger.info(log_str)
                    merge_start_dt = datetime.datetime.now()
                    _perform_merge(HH_re_list=HH_re_list,
                                   HH_merged_list=HH_merged_list,
                                   XXX_re_list=XXX_re_list,
                                   XXX_merged_list=XXX_merged_list,
                                   input_folder_path=input_folder_path,
                                   fn_template=fn_template,
                                   output_folder_path=output_folder_path,
                                   cleanup=cleanup)
                    merge_end_dt = datetime.datetime.now()
                    logger.debug(merge_end_dt)
                    merge_elapse_dt = merge_end_dt - merge_start_dt
                    logger.info("Done in {merge_elapse_dt}".format(merge_elapse_dt=str(merge_elapse_dt)))

                else:  # forecast
                    model_configuration_folder_name = model_cfg
                    input_folder_path = os.path.join(input_simulation_folder_path, model_configuration_folder_name)
                    output_folder_path = os.path.join(output_simulation_folder_path, model_configuration_folder_name)
                    for data_type in data_type_list:
                        if "analysis_assim" == model_cfg:
                            HH_re_list = ["\d\d"]
                            HH_merged_list = ["ALL"]
                            XXX_re_list = ["00", "01", "02"]
                            XXX_merged_list = ["00", "01", "02"]

                            if data_type == "channel":
                                fn_template = "nwm.t{HH}z.analysis_assim.channel_rt.tm{XXX}.conus.nc"
                            elif data_type == "land":
                                fn_template = "nwm.t{HH}z.analysis_assim.land.tm{XXX}.conus.nc"
                            elif data_type == "reservoir":
                                fn_template = "nwm.t{HH}z.analysis_assim.reservoir.tm{XXX}.conus.nc"
                            elif data_type == "terrain":
                                fn_template = "nwm.t{HH}z.analysis_assim.terrain_rt.tm{XXX}.conus.nc"
                        elif "short_range" == model_cfg:
                            HH_re_list = [str(i).zfill(2) for i in range(0, 24)]
                            HH_merged_list = HH_re_list
                            XXX_re_list = ["\d\d\d"]
                            XXX_merged_list = ["ALL"]

                            if data_type == "channel":
                                fn_template = "nwm.t{HH}z.short_range.channel_rt.f{XXX}.conus.nc"
                            elif data_type == "land":
                                fn_template = "nwm.t{HH}z.short_range.land.f{XXX}.conus.nc"
                            elif data_type == "reservoir":
                                fn_template = "nwm.t{HH}z.short_range.reservoir.f{XXX}.conus.nc"
                            elif data_type == "terrain":
                                fn_template = "nwm.t{HH}z.short_range.terrain_rt.f{XXX}.conus.nc"
                        elif "medium_range" == model_cfg:
                            HH_re_list = [str(i).zfill(2) for i in range(0, 19, 6)]
                            HH_merged_list = HH_re_list
                            XXX_re_list = ["\d\d\d"]
                            XXX_merged_list = ["ALL"]

                            if data_type == "channel":
                                fn_template = "nwm.t{HH}z.medium_range.channel_rt.f{XXX}.conus.nc"
                            elif data_type == "land":
                                fn_template = "nwm.t{HH}z.medium_range.land.f{XXX}.conus.nc"
                            elif data_type == "reservoir":
                                fn_template = "nwm.t{HH}z.medium_range.reservoir.f{XXX}.conus.nc"
                            elif data_type == "terrain":
                                fn_template = "nwm.t{HH}z.medium_range.terrain_rt.f{XXX}.conus.nc"
                        elif "long_range_mem" in model_cfg:
                            mem_id = int(model_cfg[-1])
                            HH_re_list = [str(i).zfill(2) for i in range(0, 19, 6)]
                            HH_merged_list = HH_re_list
                            XXX_re_list = ["\d\d\d"]
                            XXX_merged_list = ["ALL"]

                            if data_type == "channel":
                                fn_template = "nwm.t{HH}z.long_range.channel_rt_" + str(mem_id) + ".f{XXX}.conus.nc"
                            elif data_type == "land":
                                fn_template = "nwm.t{HH}z.long_range.land_" + str(mem_id) + ".f{XXX}.conus.nc"
                            elif data_type == "reservoir":
                                fn_template = "nwm.t{HH}z.long_range.reservoir_" + str(mem_id) + ".f{XXX}.conus.nc"
                            elif data_type == "terrain":
                                continue
                        log_str = "Merging {file_type}-{model_cfg}-{data_type}".format(file_type=file_type,
                                                                                       model_cfg=model_cfg,
                                                                                       data_type=data_type)
                        logger.info(log_str)
                        merge_start_dt = datetime.datetime.now()
                        _perform_merge(HH_re_list=HH_re_list,
                                       HH_merged_list=HH_merged_list,
                                       XXX_re_list=XXX_re_list,
                                       XXX_merged_list=XXX_merged_list,
                                       input_folder_path=input_folder_path,
                                       fn_template=fn_template,
                                       output_folder_path=output_folder_path,
                                       cleanup=cleanup)
                        merge_end_dt = datetime.datetime.now()
                        logger.debug(merge_end_dt)
                        merge_elapse_dt = merge_end_dt - merge_start_dt
                        logger.info("Done in {merge_elapse_dt}".format(merge_elapse_dt=str(merge_elapse_dt)))


    # merge daily analysis_assim to one file
    logger.debug("Prepare to merge daily analysis_assim into one if needed...")
    r = re.compile(r"nwm.20\d\d\d\d\d\d")
    date_dir_name_list = filter(lambda x: os.path.isdir(os.path.join(output_base_path, x)) and r.match(x),
                                os.listdir(output_base_path))
    date_dir_name_list.sort(key=lambda x: int(x.split('.')[1]))

    for config_name in ["analysis_assim", "forcing_analysis_assim"]:
        for geometry in ["channel_rt", "reservoir", "land", "terrain_rt", "forcing"]:
            for tm in ["00", "01", "02"]:
                file_list = []
                merged_file_name = "nwm.tALLz.analysis_assim.{geometry}.tm{tm}.conus.nc".format(geometry=geometry,
                                                                                                tm=tm)
                merged_file_path = os.path.join(output_base_path, merged_file_name)
                for date_dir_name in date_dir_name_list:
                    path = os.path.join(output_base_path, date_dir_name, config_name,
                                        "nwm.tALLz.analysis_assim.{geometry}.tm{tm}.conus.nc".format(geometry=geometry,
                                                                                                     tm=tm))
                    file_list = file_list + glob.glob(path)

                # merge netcdf
                if len(file_list) <= 1:
                    continue
                logger.debug("Merging daily analysis_assim into one...")
                file_list.sort()
                ncrcat_cmd = ["ncrcat", "-h"]

                ncrcat_cmd.append("-o")
                ncrcat_cmd.append(merged_file_path)

                for item_file_path in file_list:
                    ncrcat_cmd.append(item_file_path)

                try:
                    proc = subprocess.Popen(ncrcat_cmd,
                                            stdin=subprocess.PIPE,
                                            stdout=subprocess.PIPE,
                                            stderr=subprocess.PIPE,
                                            )
                    proc.wait()
                    stdout, stderr = proc.communicate()
                    if stdout:
                        logger.debug(stdout.rstrip())
                    if stderr:
                        if "INFO/WARNING".lower() in stderr.lower():
                            logger.debug(stderr.rstrip())
                        else:
                            logger.error(stderr.rstrip())
                            logger.error(str(ncrcat_cmd))

                except Exception as ex:
                    logger.error(ex.message)
                    logger.error(str(ncrcat_cmd))

    pass


def _perform_merge(HH_re_list=None,
                   HH_merged_list=None,
                   XXX_re_list=None,
                   XXX_merged_list=None,
                   input_folder_path=None,
                   fn_template=None,
                   output_folder_path=None,
                   cleanup=True):

    ncrcat_cmd_base = ["ncrcat", "-h"]
    for i in range(len(HH_re_list)):
        HH_re = HH_re_list[i]
        HH_merged = HH_merged_list[i]
        for j in range(len(XXX_re_list)):
            rm_list = []
            ncrcat_cmd = copy.copy(ncrcat_cmd_base)
            XXX_re = XXX_re_list[j]
            XXX_merged = XXX_merged_list[j]

            re_pattern = fn_template.format(HH=HH_re, XXX=XXX_re)
            pattern = re.compile(re_pattern)

            if os.path.exists(input_folder_path):
                folder_content_list = os.listdir(input_folder_path)
                fn_list = [fn for fn in folder_content_list if pattern.match(fn)]
                # only perform merge on 2+ files
                if len(fn_list) < 2:
                    continue
                fn_list.sort()
                fn_merged = fn_template.format(HH=HH_merged, XXX=XXX_merged)
                fn_merged_path = os.path.join(output_folder_path, fn_merged)
                ncrcat_cmd.append("-o")
                ncrcat_cmd.append(fn_merged_path)
                for fn in fn_list:
                    fn_path = os.path.join(input_folder_path, fn)
                    ncrcat_cmd.append(fn_path)
                    rm_list.append(fn_path)
                try:
                    proc = subprocess.Popen(ncrcat_cmd,
                                            stdin=subprocess.PIPE,
                                            stdout=subprocess.PIPE,
                                            stderr=subprocess.PIPE,
                                            )
                    proc.wait()
                    stdout, stderr = proc.communicate()
                    if stdout:
                        logger.debug(stdout.rstrip())
                    if stderr:
                        if "INFO/WARNING".lower() in stderr.lower():
                            logger.debug(stderr.rstrip())
                        else:
                            logger.error(stderr.rstrip())
                            logger.error(str(ncrcat_cmd))

                    if cleanup:
                        for f in rm_list:
                            os.remove(f)

                except Exception as ex:
                    logger.error(ex.message)
                    logger.error(str(ncrcat_cmd))
    pass
