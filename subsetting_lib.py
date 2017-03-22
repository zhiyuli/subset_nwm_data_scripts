import netCDF4
import os
import numpy
import logging
import re
import copy
import subprocess

logger = logging.getLogger('subset_netcdf')

def subset_grid_file(in_nc_file=None, out_nc_file=None, grid_dict=None,
                     template_version="v1.1", netcdf_format="NETCDF4_CLASSIC"):
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


def subset_comid_file(in_nc_file=None, out_nc_file=None, comid_list=None, index_list=None, reuse_comid_and_index=False,
                     template_version="v1.1", netcdf_format="NETCDF4_CLASSIC"):
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
                            var_obj[:] = comid_list_np
                        else:
                            # v1.1: hydrologic parameter
                            var_obj[:] = in_nc.variables[name][index_list]
                    elif len(var_obj.dimensions) == 2:
                        if var_obj.dimensions[0] == "time" and var_obj.dimensions[1] == "feature_id":
                            # merge: hydrologic parameter
                            var_obj[0][:] = in_nc.variables[name][index_list]
                        elif var_obj.dimensions[0] == "time" and var_obj.dimensions[1] == "reference_time":
                            # merge: reference_time
                            var_obj[0][:] = in_nc.variables[name][:]
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
        return comid_list, index_list


def merge_netcdf(input_base_path=None, output_base_path=None, cleanup=True):

    if output_base_path is None:
        output_base_path = input_base_path
    folder_list = ["forcing_analysis_assim", "forcing_short_range", "forcing_medium_range",
                    "analysis_assim", "short_range", "medium_range",
                    "long_range_mem1",  "long_range_mem2",  "long_range_mem3",  "long_range_mem4"]
    #folder_list = ["forcing_medium_range"]

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

            merge(HH_re_list=HH_re_list, HH_merged_list=HH_merged_list, XXX_re_list=XXX_re_list,
                  XXX_merged_list=XXX_merged_list, input_base_path=input_base_path, model=model,
                  fn_template=fn_template, output_base_path=output_base_path, cleanup=cleanup)
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
                log_str = "Merging {model}".format(model=model)
                logger.info(log_str)
                merge(HH_re_list=HH_re_list, HH_merged_list=HH_merged_list, XXX_re_list=XXX_re_list,
                      XXX_merged_list=XXX_merged_list, input_base_path=input_base_path, model=model,
                      fn_template=fn_template, output_base_path=output_base_path, cleanup=cleanup)

    pass


def merge(HH_re_list=None, HH_merged_list=None, XXX_re_list=None, XXX_merged_list=None, input_base_path=None, model=None,
         fn_template=None, output_base_path=None, cleanup=True):

    ncrcat_cmd_base = ["ncrcat", "-h"]
    rm_cmd_base = ["rm", "-f"]
    for i in range(len(HH_re_list)):
        HH_re = HH_re_list[i]
        HH_merged = HH_merged_list[i]
        for j in range(len(XXX_re_list)):
            rm_cmd = copy.copy(rm_cmd_base)
            ncrcat_cmd = copy.copy(ncrcat_cmd_base)
            XXX_re = XXX_re_list[j]
            XXX_merged = XXX_merged_list[j]

            re_pattern = fn_template.format(HH=HH_re, XXX=XXX_re)
            pattern = re.compile(re_pattern)
            data_folder_path = os.path.join(input_base_path, model)
            if os.path.exists(data_folder_path):
                folder_content_list = os.listdir(data_folder_path)
                fn_list = [fn for fn in folder_content_list if pattern.match(fn)]
                if len(fn_list) == 0:
                    continue
                fn_list.sort()
                fn_merged = fn_template.format(HH=HH_merged, XXX=XXX_merged)
                fn_merged_path = os.path.join(output_base_path, model, fn_merged)
                ncrcat_cmd.append("-o")
                ncrcat_cmd.append(fn_merged_path)
                for fn in fn_list:
                    fn_path = os.path.join(data_folder_path, fn)
                    ncrcat_cmd.append(fn_path)
                    rm_cmd.append(fn_path)
                try:
                    proc = subprocess.Popen(ncrcat_cmd,
                                            stdin=subprocess.PIPE,
                                            stdout=subprocess.PIPE,
                                            stderr=subprocess.PIPE,
                                            )
                    proc.wait()
                    stdout, stderr = proc.communicate()
                    if stdout:
                        logger.error(stdout)
                        logger.error(str(ncrcat_cmd))
                    if stderr:
                        if "INFO/WARNING".lower() in stderr.lower():
                            logger.debug(stderr)
                            logger.debug(str(ncrcat_cmd))
                        else:
                            logger.error(stderr)
                            logger.error(str(ncrcat_cmd))

                    if cleanup:
                        proc = subprocess.Popen(rm_cmd,
                                                stdin=subprocess.PIPE,
                                                stdout=subprocess.PIPE,
                                                stderr=subprocess.PIPE,
                                                )
                        proc.wait()
                        stdout, stderr = proc.communicate()
                        if stdout:
                            logger.error(stdout)
                            logger.error(str(ncrcat_cmd))
                        if stderr:
                            logger.error(stderr)
                            logger.error(str(ncrcat_cmd))

                    # proc = subprocess.call(ncrcat_cmd)
                    # proc = subprocess.call(rm_cmd)

                except Exception as ex:
                    logger.error(ex.message)
                    logger.error(str(ncrcat_cmd))
                pass