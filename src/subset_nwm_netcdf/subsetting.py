import os
import subprocess
import shutil
import datetime
import platform
import re
import copy
import logging

import numpy
import netCDF4


logger = logging.getLogger('subset_nwm_netcdf')
default_sed_win_path = os.path.join(os.path.dirname(__file__), "static/sed_win/sed.exe")
default_nc_templates_path = os.path.join(os.path.dirname(__file__), "static/netcdf_templates")

def _render_cdl_file(content_list=[], file_path=None):
    for item in content_list:
        _replace_text_in_file(search_text=item[0], replace_text=item[1], file_path=file_path)


def _replace_text_in_file(search_text=None, replace_text=None, file_path=None):
    if "windows" in platform.system().lower():
        sed_cmd = [default_sed_win_path, "-i", "s/{0}/{1}/g".format(search_text, replace_text), file_path]
    else:
        sed_cmd = ['sed', '-i', 's/{0}/{1}/g'.format(search_text, replace_text), file_path]

    proc = subprocess.Popen(sed_cmd,
                            stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            )
    proc.wait()
    stdout, stderr = proc.communicate()

    if "windows" in platform.system().lower():
        # remove 'sedXXXXX' files created by sed, which is a unresolved bug of gnu sed on windows
        # see: https://sourceforge.net/p/gnuwin32/bugs/500/
        pattern = re.compile("sed\w\w\w\w\w")
        folder_content_list = os.listdir("./")
        [os.remove(fn) for fn in folder_content_list if pattern.match(fn) and os.path.isfile(fn)]

    if stdout:
        logger.error(stdout.rstrip())
        raise Exception(" replace_text_in_file() error @ {0}".format(file_path))
    if stderr:
        logger.error(stderr.rstrip())
        raise Exception(" replace_text_in_file() error @ {0}".format(file_path))


def _create_nc_from_cdf(cdl_file=None, out_file=None, remove_cdl=True):
    # -7: netcdf-4 classic model
    # subprocess.call(['ncgen', '-7', '-o', out_file, cdl_file])
    ncgen_cmd = ['ncgen', '-7', '-o', out_file, cdl_file]
    proc = subprocess.Popen(ncgen_cmd,
                            stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            )
    proc.wait()
    stdout, stderr = proc.communicate()
    if stdout:
        logger.error(stdout.rstrip())
        raise Exception(" create_nc_from_cdf() error @ {0}".format(cdl_file))
    if stderr:
        logger.error(stderr.rstrip())
        raise Exception(" create_nc_from_cdf() error @ {0}".format(cdl_file))

    if remove_cdl:
        os.remove(cdl_file)


def _subset_nwm_netcdf(job_id=None,
                        grid=None,
                        comid_list=None,
                        simulation_date=None,
                        data_type=None,
                        model_type=None,
                        file_type=None,
                        time_stamp_list=None,
                        input_folder_path=None,
                        output_folder_path=None,
                        template_folder_path=None,
                        template_version="v1.1",
                        write_file_list=None,
                        use_merge_template=False,
                        cleanup=False,
                        use_chunked_template=True):

    if job_id is None:
        job_id = "NO_JOB_ID_PROVIDED"
    output_folder_path = os.path.join(output_folder_path, job_id)
    data_type = data_type.lower()
    model_type = model_type.lower() if model_type else None
    file_type = file_type.lower() if file_type else None
    template_version = template_version.lower()

    dim_x_len = grid['maxX'] - grid['minX'] + 1
    dim_y_len = grid['maxY'] - grid['minY'] + 1

    # index_x_old = index_x_new + index_offset_x
    index_offset_x = grid["minX"]
    # index_y_old = index_y_new + index_offset_y
    index_offset_y = grid["minY"]

    var_list = []
    if data_type == "forcing":
        if model_type == "analysis_assim":
            cdl_template_filename = "nwm.tHHz.analysis_assim.forcing.tm00.conus.cdl_template"
            var_list.append(["HH", range(24)])  # 00, 01, ... 23
        elif model_type == "short_range":
            cdl_template_filename = "nwm.tHHz.short_range.forcing.fXXX.conus.cdl_template"
            var_list.append(["HH", range(24)])  # 00, 01, ... 23
            var_list.append(["XXX", range(1, 19)])  # 001, 0002 ... 018
        elif model_type == "medium_range":
            cdl_template_filename = "nwm.tHHz.medium_range.forcing.fXXX.conus.cdl_template"
            var_list.append(["HH", range(0, 19, 6)])  # 00, 06, 12, 18
            var_list.append(["XXX", range(1, 241)])  # 001, 002, .... 240
        elif model_type == "long_range":
            raise Exception("Long-range forecast has no dedicated forcing files.")

    elif data_type == "forecast":
        if model_type == "analysis_assim":
            var_list.append(["HH", range(24)])  # 00, 01, 02...23

            if file_type == "channel":
                cdl_template_filename = "nwm.tHHz.analysis_assim.channel_rt.tm00.conus.cdl_template"
            elif file_type == "land":
                cdl_template_filename = "nwm.tHHz.analysis_assim.land.tm00.conus.cdl_template"
            elif file_type == "reservoir":
                cdl_template_filename = "nwm.tHHz.analysis_assim.reservoir.tm00.conus.cdl_template"
            elif file_type == "terrain":
                raise NotImplementedError()

        elif model_type == "short_range":
            var_list.append(["HH", range(24)])  # 00, 01, ... 23
            var_list.append(["XXX", range(1, 19)])  # 001, 002 ... 18

            if file_type == "channel":
                cdl_template_filename = "nwm.tHHz.short_range.channel_rt.fXXX.conus.cdl_template"
            elif file_type == "land":
                cdl_template_filename = "nwm.tHHz.short_range.land.fXXX.conus.cdl_template"
            elif file_type == "reservoir":
                cdl_template_filename = "nwm.tHHz.short_range.reservoir.fXXX.conus.cdl_template"
            elif file_type == "terrain":
                raise NotImplementedError()

        elif model_type == "medium_range":
            var_list.append(["HH", range(0, 19, 6)])  # 00, 06, ... 18
            var_list.append(["XXX", range(3, 241, 3)])  # 003, 006, 009, ... 240

            if file_type == "channel":
                cdl_template_filename = "nwm.tHHz.medium_range.channel_rt.fXXX.conus.cdl_template"
            elif file_type == "land":
                cdl_template_filename = "nwm.tHHz.medium_range.land.fXXX.conus.cdl_template"
            elif file_type == "reservoir":
                cdl_template_filename = "nwm.tHHz.medium_range.reservoir.fXXX.conus.cdl_template"
            elif file_type == "terrain":
                raise NotImplementedError()

        elif "long_range_mem" in model_type:
            mem_id = int(model_type[-1])
            if mem_id in [1, 2, 3, 4]:
                var_list.append(["M", [mem_id]])  # 1, 2, 3, 4
                var_list.append(["HH", range(0, 19, 6)])  # 00, 06, 12, 18

                if file_type == "channel":
                    var_list.append(["XXX", range(6, 721, 6)])  # 006, 012, 018, ... 720
                    cdl_template_filename = "nwm.tHHz.long_range.channel_rt_M.fXXX.conus.cdl_template"
                elif file_type == "land":
                    var_list.append(["XXX", range(24, 721, 24)])  # 024, 048, ... 720
                    cdl_template_filename = "nwm.tHHz.long_range.land_M.fXXX.conus.cdl_template"
                elif file_type == "reservoir":
                    var_list.append(["XXX", range(6, 721, 6)])  # 006, 012, 018, ... 720
                    cdl_template_filename = "nwm.tHHz.long_range.reservoir_M.fXXX.conus.cdl_template"
                elif file_type == "terrain":
                    raise NotImplementedError("terrain")
            else:
                raise Exception("Invalid long_rang model type @: {0}".format(model_type))
        else:
            raise Exception("Invalid long_rang model type @: {0}".format(model_type))

    else:
        raise Exception("invalid data_type: {0}".format(data_type))

    if use_chunked_template:
        cdl_template_filename += "_chunked"

    if use_merge_template:
        cdl_template_filename += "_merge"

    if "long_range_mem" in model_type:
        # long_range uses same templates for all mem1-mem4
        cdl_template_file_path = os.path.join(template_folder_path, template_version,
                                     data_type, "long_range", cdl_template_filename)
    else:
        cdl_template_file_path = os.path.join(template_folder_path, template_version,
                                     data_type, model_type, cdl_template_filename)

    if not os.path.isfile(cdl_template_file_path):
        raise Exception("template file missing @: {0}".format(cdl_template_file_path))

    cdl_filename = cdl_template_filename[0:cdl_template_filename.rfind("_template")]
    nc_template_file_name = cdl_filename.replace(".cdl", ".nc")
    out_nc_folder_template_path = os.path.join(output_folder_path, "nc_template")
    nc_template_file_path = os.path.join(out_nc_folder_template_path, nc_template_file_name)

    if type(time_stamp_list) is list and len(time_stamp_list) > 0:
        for var in var_list:
            if var[0] == "HH":
                var[1] = time_stamp_list
                break
    nc_file_name = nc_template_file_name
    nc_filename_list = [nc_file_name]
    for var in var_list:
        var_name = var[0]
        var_range = var[1]
        nc_filename_list_new = []
        for nc in nc_filename_list:
            for var_value in var_range:
                var_value_str = str(var_value).zfill(len(var_name))
                nc_new = nc
                nc_new = nc_new.replace(var_name, var_value_str)
                nc_filename_list_new.append(nc_new)
        nc_filename_list = nc_filename_list_new

    # write wget download list file
    # write_file_list = None
    # write_file_list = {"url_base": "http://para.nomads.ncep.noaa.gov/pub/data/nccf/com/nwm/para/",
    #                     "save_to_path_base": "/projects/water/nwm/new_data/pub/data/nccf/com/nwm/para/"}
    # write_file_list = {"url_base": "http://para.nomads.ncep.noaa.gov/pub/data/nccf/com/nwm/para/",
    #                    "save_to_path_base": "/cygdrive/f/nwm_new_data/"}

    if type(write_file_list) is dict:
        url_base = write_file_list["url_base"] + "nwm.$simulation_date/"
        save_to_path_base = os.path.join(write_file_list["save_to_path_base"],
                                         "nwm.simulation_date")
        # -nc: do not download file if it already exists on disk
        wget_cmd_template = "wget -nc -P {save_to_folder_path} {url}"
        write_file_list_path = "./file_list_{0}.txt".format(job_id)
        with open(write_file_list_path, "a+") as f:
            if os.path.getsize(write_file_list_path) == 0:
                f.write("simulation_date={simulation_date}\n".format(simulation_date=simulation_date))
            for nc_filename in nc_filename_list:
                save_to_folder_path = os.path.join(save_to_path_base, model_type)
                url = url_base + model_type + "/" + nc_filename
                if data_type == "forcing":
                    save_to_folder_path = os.path.join(save_to_path_base, data_type + "_" + model_type)
                    url = url_base + data_type + "_" + model_type + "/" + nc_filename
                save_to_folder_path = save_to_folder_path.replace("simulation_date", "$simulation_date")
                wget_cmd = wget_cmd_template.format(save_to_folder_path=save_to_folder_path, url=url)
                f.write(wget_cmd + "\n")
        logger.info("file list written to {0}".format(write_file_list_path))
        return

    simulation_date = "nwm." + simulation_date
    out_nc_folder_path = os.path.join(output_folder_path, simulation_date, model_type)
    in_nc_folder_path = os.path.join(input_folder_path, simulation_date, model_type)
    if data_type == "forcing":
        out_nc_folder_path = os.path.join(output_folder_path, simulation_date, data_type + "_" + model_type)
        in_nc_folder_path = os.path.join(input_folder_path, simulation_date, data_type + "_" + model_type)

    if not os.path.exists(out_nc_folder_path):
        os.makedirs(out_nc_folder_path)

    comid_list_dict = {"channel": None, "reservoir": None}
    direct_read = None
    comid_list_dict[file_type] = comid_list
    index_list_dict = {"channel": None, "reservoir": None}
    for nc_filename in nc_filename_list:
        out_nc_file = os.path.join(out_nc_folder_path, nc_filename)
        in_nc_file = os.path.join(in_nc_folder_path, nc_filename)
        if not os.path.isfile(in_nc_file):
            logger.warning("Original netcdf missing @: {0}".format(in_nc_file))
            # skip this netcdf as its original file is missing
            continue

        if os.path.isfile(out_nc_file):
            logger.debug("Overwriting existing nc file @: {0}".format(out_nc_file))

        # render a template nc file and put it under output folder
        if not os.path.isfile(nc_template_file_path):
            if not os.path.exists(out_nc_folder_template_path):
                os.makedirs(out_nc_folder_template_path)
            cdl_file_path = os.path.join(out_nc_folder_template_path, cdl_filename)
            shutil.copyfile(cdl_template_file_path, cdl_file_path)

            content_list = []
            if data_type == "forecast" and file_type in ["channel", "reservoir"]:
                if file_type == "channel" and len(comid_list) == 0:
                    return
                if file_type == "reservoir" and len(comid_list) == 0:
                    return
                content_list.append(["{%feature_id%}", str(len(comid_list))])
            elif data_type == "forcing" or \
                    (data_type == "forecast" and file_type in ["land"]):
                content_list.append(["{%x%}", str(dim_x_len)])
                content_list.append(["{%y%}", str(dim_y_len)])

            content_list.append(["{%filename%}", nc_template_file_name])
            content_list.append(["{%model_initialization_time%}", "2030-01-01_00:00:00"])
            content_list.append(["{%model_output_valid_time%}", "2030-01-01_00:00:00"])

            _render_cdl_file(content_list=content_list, file_path=cdl_file_path)
            _create_nc_from_cdf(cdl_file=cdl_file_path, out_file=nc_template_file_path)

        shutil.copyfile(nc_template_file_path, out_nc_file)

        if data_type == "forcing" or \
           data_type == "forecast" and file_type == "land":

            _subset_grid_file(in_nc_file=in_nc_file,
                              out_nc_file=out_nc_file,
                              grid_dict=grid)

        elif data_type == "forecast" and (file_type == "channel" or file_type == "reservoir"):

            comid_list_dict[file_type], index_list_dict[file_type], direct_read = \
                _subset_comid_file(in_nc_file=in_nc_file,
                                   out_nc_file=out_nc_file,
                                   comid_list=comid_list_dict[file_type],
                                   index_list=index_list_dict[file_type],
                                   reuse_comid_and_index=True,
                                   direct_read=direct_read)
    if cleanup:
        # remove nc_template folder
        shutil.rmtree(out_nc_folder_template_path)


def start_subset_nwm_netcdf_job(job_id=None,
                                netcdf_folder_path=None,
                                output_folder_path=None,
                                simulation_date_list=None,
                                data_type_list=None,
                                model_type_list=None,
                                file_type_list=None,
                                time_stamp_list=None,
                                grid_dict=None,
                                stream_comid_list=None,
                                reservoir_comid_list=None,
                                merge_netcdfs=True,
                                cleanup=True,
                                template_folder_path=None,
                                write_file_list=None,
                                template_version="v1.1",
                                use_chunked_template=True):
    """

    :param job_id: used as result folder name
    :param netcdf_folder_path: original NWM netcdf folder
    :param output_folder_path: output result base path
    :param template_folder_path: optional, path to netcdf template folder
    :param simulation_date_list: list of date strings ["20170327", "20170328"]
    :param data_type_list:  ["forecast", 'forcing']
    :param model_type_list: ['analysis_assim', 'short_range', 'medium_range', 'long_range'],
                            "long_range": long_range_mem1-4,
                            "long_range_mem4": indicate a specific long_range_mem model
    :param file_type_list: ['channel', 'reservoir', 'land']
    :param time_stamp_list: [1, 2, ...];  [] or None means all default time stamps
    :param grid_dict: {"minX": 11, "maxX": 22, "minY": 33, "maxY": 44}
    :param stream_comid_list: [comid1, comid2, ...]
    :param reservoir_comid_list: [comid1, comid2, ....]
    :param merge_netcdfs: True: merge netcdf after subsetting
    :param cleanup: remove intermediate files and only keep merged netcdfs
    :param write_file_list: internal testing purpose, ignore this parameter
    :param template_version: "v1.1"
    :param use_chunked_template: True: default
    :return: no value returned
    """

    logger.warn("NetCDF utilities and NCO commands should be discoverable in system path")
    if template_folder_path is None:
        template_folder_path = default_nc_templates_path
    logger.info("---------------Subsetting {0}----------------".format(job_id))
    start_dt = datetime.datetime.now()
    logger.info(start_dt)

    logger.info("job_id={0}".format(str(job_id)))
    logger.info("netcdf_folder_path={0}".format(str(netcdf_folder_path)))
    logger.info("output_folder_path={0}".format(str(output_folder_path)))
    logger.info("template_folder_path={0}".format(str(template_folder_path)))
    logger.info("simulation_date_list={0}".format(str(simulation_date_list)))
    logger.info("data_type_list={0}".format(str(data_type_list)))
    logger.info("model_type_list={0}".format(str(model_type_list)))
    logger.info("file_type_list={0}".format(str(file_type_list)))
    logger.info("grid_dict={0}".format(str(grid_dict)))
    logger.info("stream_comid_list={0}".format(str(stream_comid_list)))
    logger.info("reservoir_comid_list={0}".format(str(reservoir_comid_list)))
    logger.info("merge_netcdfs={0}".format(str(merge_netcdfs)))
    logger.info("cleanup={0}".format(str(cleanup)))
    logger.info("write_file_list={0}".format(str(write_file_list)))
    logger.info("template_version={0}".format(str(template_version)))
    logger.info("use_chunked_template={0}".format(use_chunked_template))

    if "long_range" in model_type_list:
        model_type_list.remove("long_range")
        for i in range(1, 5):
            model_type_list.append("long_range_mem{0}".format(str(i)))

    subset_work_dict = {'simulation_date': simulation_date_list,
                        'date_type': data_type_list,
                        'model_type': model_type_list,
                        'file_type':  file_type_list
                        }

    for simulation_date in subset_work_dict["simulation_date"]:
        logger.info("-------------Subsetting {0}--------------------".format(simulation_date))
        for data_type in subset_work_dict["date_type"]:
            for model_type in subset_work_dict["model_type"]:
                data_type_list_copy = subset_work_dict['file_type']
                if data_type == "forcing":
                    data_type_list_copy = [None]
                    if "long_range" in model_type:
                        # long_range has no dedicated forcing files
                        continue
                for file_type in data_type_list_copy:
                    try:
                        comid_list = stream_comid_list
                        if 'reservoir' == file_type:
                            comid_list = reservoir_comid_list
                        log_str = "Working on: {simulation_date}-{data_type}-{model_type}-{file_type}".\
                            format(simulation_date=simulation_date, data_type=data_type,
                                   model_type=model_type, file_type=file_type)
                        logger.info(log_str)
                        sim_start_dt = datetime.datetime.now()
                        logger.debug(sim_start_dt)

                        _subset_nwm_netcdf(job_id=job_id,
                                           grid=grid_dict,
                                           comid_list=comid_list,
                                           simulation_date=simulation_date,
                                           data_type=data_type,
                                           model_type=model_type,
                                           file_type=file_type,
                                           time_stamp_list=time_stamp_list,
                                           input_folder_path=netcdf_folder_path,
                                           output_folder_path=output_folder_path,
                                           template_folder_path=template_folder_path,
                                           template_version=template_version,
                                           write_file_list=write_file_list,
                                           use_merge_template=merge_netcdfs,
                                           cleanup=cleanup,
                                           use_chunked_template=use_chunked_template)
                        sim_end_dt = datetime.datetime.now()
                        logger.debug(sim_end_dt)
                        sim_elapsed = sim_end_dt - sim_start_dt
                        logger.info("Done in {0}; Subsetting Elapsed: {1}".format(sim_elapsed, sim_end_dt - start_dt))
                    except Exception as ex:
                        logger.error(str(type(ex)) + ex.message)

    end_dt = datetime.datetime.now()
    logger.debug(end_dt)
    elapse_dt = end_dt - start_dt
    logger.info(elapse_dt)
    logger.info("---------------------Subsetting Done-----------------------------")

    if merge_netcdfs:
        logger.info("---------------------Start Merging-----------------------------")
        merge_start_dt = datetime.datetime.now()
        logger.debug(merge_start_dt)
        for simulation_date in subset_work_dict["simulation_date"]:
            logger.info("----------------------Merging {0}".format(simulation_date))
            merge_nwm_netcdf(input_base_path=os.path.join(output_folder_path, job_id, "nwm.{0}".format(simulation_date)),
                             cleanup=cleanup)
        merge_end_dt = datetime.datetime.now()
        logger.debug(merge_end_dt)
        merge_elapse_dt = merge_end_dt - merge_start_dt
        logger.info(merge_elapse_dt)
        logger.info("---------------------Merge Done-----------------------------")
    pass


def _subset_grid_file(in_nc_file=None,
                      out_nc_file=None,
                      grid_dict=None,
                      template_version="v1.1",
                      netcdf_format="NETCDF4_CLASSIC"):
    try:
        if not os.path.isfile(in_nc_file):
            raise Exception("in_nc_file missing @: {0}".format(in_nc_file))
        elif not os.path.isfile(out_nc_file):
            raise Exception("out_nc_file missing @: {0}".format(out_nc_file))

        grid = copy.copy(grid_dict)
        with netCDF4.Dataset(in_nc_file, mode='r', format=netcdf_format) as in_nc:
            with netCDF4.Dataset(out_nc_file, mode='r+', format=netcdf_format) as out_nc:

                out_nc.model_initialization_time = in_nc.model_initialization_time
                out_nc.model_output_valid_time = in_nc.model_output_valid_time
                for name, var_obj in out_nc.variables.iteritems():

                    if name in ['x', 'y']:
                        var_x_or_y = var_obj
                        var_x_or_y[:] = in_nc.variables[name][grid['min{0}'.format(name.upper())]:
                                                              grid['max{0}'.format(name.upper())] + 1]

                    elif len(var_obj.dimensions) == 2:
                        if var_obj.dimensions[0] != "time":
                            raise Exception("unexpected variable")
                        var_obj[0][:] = in_nc.variables[name][:]

                    elif len(var_obj.dimensions) == 3:

                        if var_obj.dimensions[0] != "time" or var_obj.dimensions[1] != "y" \
                           or var_obj.dimensions[2] != "x":
                            raise Exception("unexpected Geo2D variable")
                        var_obj[0] = in_nc.variables[name][0,
                                                           grid['minY']:grid['maxY'] + 1,
                                                           grid['minX']:grid['maxX'] + 1]
                    elif len(var_obj.dimensions) == 4:
                        # medium range land file variable may have 4 dimensions

                        if var_obj.dimensions[0] != "time" or var_obj.dimensions[1] != "y" \
                           or var_obj.dimensions[3] != "x":
                            raise Exception("unexpected medium range Geo2D variable")
                        var_obj[0] = in_nc.variables[name][0,
                                                           grid['minY']:grid['maxY'] + 1,
                                                           :,
                                                           grid['minX']:grid['maxX'] + 1]
                    else:
                        if len(var_obj.dimensions) > 0:
                            var_obj[:] = in_nc.variables[name][:]
    except Exception as ex:
        logger.exception(ex.message + in_nc_file)
        if os.path.isfile(out_nc_file):
            os.remove(out_nc_file)
        # if os.path.isfile(in_nc_file):
        #     os.remove(in_nc_file)


def _get_comid_indices(find_comids, all_comids):

    if type(find_comids) is not numpy.ndarray:
        find_comids = numpy.array(find_comids)
    sorted_index = all_comids.argsort()
    sorted_all_comids = all_comids[sorted_index]
    found_index_sorted = numpy.searchsorted(sorted_all_comids, find_comids)
    index = sorted_index[found_index_sorted]
    return index


def _test_read_indirect_direct(direct_read=None, in_nc=None, var_name=None, index_list=None):

    if direct_read is None:
        # test which reading mode is faster, direct or indirect
        s = datetime.datetime.now()
        all_data = in_nc.variables[var_name][:]
        e = datetime.datetime.now()
        t_indirect = e - s

        s = datetime.datetime.now()
        slice_data = in_nc.variables[var_name][index_list]
        e = datetime.datetime.now()
        t_direct = e - s

        if t_direct < t_indirect:
            direct_read = True
        else:
            direct_read = False
    return direct_read


def _subset_comid_file(in_nc_file=None,
                       out_nc_file=None,
                       comid_list=None,
                       index_list=None,
                       reuse_comid_and_index=False,
                       template_version="v1.1",
                       netcdf_format="NETCDF4_CLASSIC",
                       direct_read=None):
    try:
        if not os.path.isfile(in_nc_file):
            raise Exception("in_nc_file missing @: {0}".format(in_nc_file))
        elif not os.path.isfile(out_nc_file):
            raise Exception("out_nc_file missing @: {0}".format(out_nc_file))

        with netCDF4.Dataset(in_nc_file, mode='r', format=netcdf_format) as in_nc:
            with netCDF4.Dataset(out_nc_file, mode='r+', format=netcdf_format) as out_nc:

                if not reuse_comid_and_index or \
                        index_list is None or \
                        (len(index_list) == 0 and type(index_list) is numpy.ndarray) or \
                        type(comid_list) is not numpy.ndarray:
                    comid_list_np = numpy.array(comid_list)
                    comid_list_np.sort()
                    index_list = _get_comid_indices(comid_list_np, in_nc.variables['feature_id'][:])
                else:
                    comid_list_np = comid_list

                out_nc.model_initialization_time = in_nc.model_initialization_time
                out_nc.model_output_valid_time = in_nc.model_output_valid_time
                for name, var_obj in out_nc.variables.iteritems():
                    if len(var_obj.dimensions) == 1 and var_obj.dimensions[0] == "feature_id":
                        if name == "feature_id":
                            # v1.1, merge: feature_id, lon, lat, elevation
                            var_obj = comid_list_np
                        else:
                            # test which reading mode is faster, direct or indirect
                            direct_read = _test_read_indirect_direct(direct_read=direct_read,
                                                                     in_nc=in_nc,
                                                                     var_name=name,
                                                                     index_list=index_list)

                            if direct_read:
                                # v1.1: hydrologic parameter; merge: lon lat
                                # Do not access big data from netcdf lib using non-contiguous index list
                                var_obj[:] = in_nc.variables[name][index_list]
                            else:
                                # Instead, read all data into memory then subset it using index list
                                # See speed_test.py for details
                                all_data_np = in_nc.variables[name][:]
                                var_obj[:] = all_data_np[index_list]

                    elif len(var_obj.dimensions) == 2:
                        if var_obj.dimensions[0] == "time" and var_obj.dimensions[1] == "feature_id":

                            # test which reading mode is faster, direct or indirect
                            direct_read = _test_read_indirect_direct(direct_read=direct_read,
                                                                     in_nc=in_nc,
                                                                     var_name=name,
                                                                     index_list=index_list)
                            # merge: hydrologic parameter
                            if direct_read:
                                var_obj[0] = in_nc.variables[name][index_list]
                            else:
                                all_data_np = in_nc.variables[name][:]
                                var_obj[0] = all_data_np[index_list]

                        elif var_obj.dimensions[0] == "time" and var_obj.dimensions[1] == "reference_time":
                            # merge: reference_time
                            var_obj[0] = in_nc.variables[name][:]
                        else:
                            raise Exception("invalid nc")
                    else:
                        # v1.1, merge: time; v1.1: reference_time
                        var_obj[:] = in_nc.variables[name][:]
    except Exception as ex:
        logger.exception(str(type(ex)) + ex.message + in_nc_file)
        if os.path.isfile(out_nc_file):
            os.remove(out_nc_file)
        # if os.path.isfile(in_nc_file):
        #     os.remove(in_nc_file)
    finally:
        if reuse_comid_and_index:
            return comid_list_np, index_list, direct_read
        else:
            return comid_list, None, direct_read


def merge_nwm_netcdf(input_base_path=None, output_base_path=None, cleanup=True):

    if output_base_path is None:
        output_base_path = input_base_path
    folder_list = ["forcing_analysis_assim", "forcing_short_range", "forcing_medium_range",
                    "analysis_assim", "short_range", "medium_range",
                    "long_range_mem1",  "long_range_mem2",  "long_range_mem3",  "long_range_mem4"]

    file_type_list = ["channel", "reservoir", "land"]

    for model in folder_list:
        if "forcing_" in model:
            if "analysis_assim" in model:
                fn_template = "nwm.t{HH}z.analysis_assim.forcing.tm{XXX}.conus.nc"
                HH_re_list = ["\d\d"]
                HH_merged_list = ["ALL"]
                XXX_re_list = ["00"]
                XXX_merged_list = ["00"]
            elif "short_range" in model:
                fn_template = "nwm.t{HH}z.short_range.forcing.f{XXX}.conus.nc"
                HH_re_list = [str(i).zfill(2) for i in range(0, 24)]
                HH_merged_list = HH_re_list
                XXX_re_list = ["\d\d\d"]
                XXX_merged_list = ["ALL"]
            elif "medium_range" in model:
                fn_template = "nwm.t{HH}z.medium_range.forcing.f{XXX}.conus.nc"
                HH_re_list = [str(i).zfill(2) for i in range(0, 19, 6)]
                HH_merged_list = HH_re_list
                XXX_re_list = ["\d\d\d"]
                XXX_merged_list = ["ALL"]

            _perform_merge(HH_re_list=HH_re_list,
                           HH_merged_list=HH_merged_list,
                           XXX_re_list=XXX_re_list,
                           XXX_merged_list=XXX_merged_list,
                           input_base_path=input_base_path,
                           model=model,
                           fn_template=fn_template,
                           output_base_path=output_base_path,
                           cleanup=cleanup)
        else:
            for file_type in file_type_list:
                if "analysis_assim" in model:
                    HH_re_list = ["\d\d"]
                    HH_merged_list = ["ALL"]
                    XXX_re_list = ["00"]
                    XXX_merged_list = ["00"]

                    if file_type == "channel":
                        fn_template = "nwm.t{HH}z.analysis_assim.channel_rt.tm{XXX}.conus.nc"
                    elif file_type == "land":
                        fn_template = "nwm.t{HH}z.analysis_assim.land.tm{XXX}.conus.nc"
                    elif file_type == "reservoir":
                        fn_template = "nwm.t{HH}z.analysis_assim.reservoir.tm{XXX}.conus.nc"
                    elif file_type == "terrain":
                        raise NotImplementedError()
                elif "short_range" in model:
                    HH_re_list = [str(i).zfill(2) for i in range(0, 24)]
                    HH_merged_list = HH_re_list
                    XXX_re_list = ["\d\d\d"]
                    XXX_merged_list = ["ALL"]

                    if file_type == "channel":
                        fn_template = "nwm.t{HH}z.short_range.channel_rt.f{XXX}.conus.nc"
                    elif file_type == "land":
                        fn_template = "nwm.t{HH}z.short_range.land.f{XXX}.conus.nc"
                    elif file_type == "reservoir":
                        fn_template = "nwm.t{HH}z.short_range.reservoir.f{XXX}.conus.nc"
                    elif file_type == "terrain":
                        raise NotImplementedError()
                elif "medium_range" in model:
                    HH_re_list = [str(i).zfill(2) for i in range(0, 19, 6)]
                    HH_merged_list = HH_re_list
                    XXX_re_list = ["\d\d\d"]
                    XXX_merged_list = ["ALL"]

                    if file_type == "channel":
                        fn_template = "nwm.t{HH}z.medium_range.channel_rt.f{XXX}.conus.nc"
                    elif file_type == "land":
                        fn_template = "nwm.t{HH}z.medium_range.land.f{XXX}.conus.nc"
                    elif file_type == "reservoir":
                        fn_template = "nwm.t{HH}z.medium_range.reservoir.f{XXX}.conus.nc"
                    elif file_type == "terrain":
                        raise NotImplementedError()
                elif "long_range_mem" in model:
                    mem_id = int(model[-1])
                    HH_re_list = [str(i).zfill(2) for i in range(0, 19, 6)]
                    HH_merged_list = HH_re_list
                    XXX_re_list = ["\d\d\d"]
                    XXX_merged_list = ["ALL"]

                    if file_type == "channel":
                        fn_template = "nwm.t{HH}z.long_range.channel_rt_" + str(mem_id) + ".f{XXX}.conus.nc"
                    elif file_type == "land":
                        fn_template = "nwm.t{HH}z.long_range.land_" + str(mem_id) + ".f{XXX}.conus.nc"
                    elif file_type == "reservoir":
                        fn_template = "nwm.t{HH}z.long_range.reservoir_" + str(mem_id) + ".f{XXX}.conus.nc"
                    elif file_type == "terrain":
                        raise NotImplementedError()
                log_str = "Merging {model}-{file_type}".format(model=model, file_type=file_type)
                logger.info(log_str)
                _perform_merge(HH_re_list=HH_re_list,
                               HH_merged_list=HH_merged_list,
                               XXX_re_list=XXX_re_list,
                               XXX_merged_list=XXX_merged_list,
                               input_base_path=input_base_path,
                               model=model,
                               fn_template=fn_template,
                               output_base_path=output_base_path,
                               cleanup=cleanup)
    pass


def _perform_merge(HH_re_list=None,
                   HH_merged_list=None,
                   XXX_re_list=None,
                   XXX_merged_list=None,
                   input_base_path=None,
                   model=None,
                   fn_template=None,
                   output_base_path=None,
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
            data_folder_path = os.path.join(input_base_path, model)
            if os.path.exists(data_folder_path):
                folder_content_list = os.listdir(data_folder_path)
                fn_list = [fn for fn in folder_content_list if pattern.match(fn)]
                # only perform merge on 2+ files
                if len(fn_list) < 2:
                    continue
                fn_list.sort()
                fn_merged = fn_template.format(HH=HH_merged, XXX=XXX_merged)
                fn_merged_path = os.path.join(output_base_path, model, fn_merged)
                ncrcat_cmd.append("-o")
                ncrcat_cmd.append(fn_merged_path)
                for fn in fn_list:
                    fn_path = os.path.join(data_folder_path, fn)
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
