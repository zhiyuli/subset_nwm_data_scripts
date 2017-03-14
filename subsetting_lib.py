import netCDF4
import copy
import os
import numpy
import logging

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

                    elif "v1.1" in template_version and len(var_obj.dimensions) == 3:

                        if var_obj.dimensions[0] != "time" or var_obj.dimensions[1] != "y" \
                           or var_obj.dimensions[2] != "x":
                            raise Exception("unexpected Geo2D variable")
                        var_obj[0] = in_nc.variables[name][0,
                                                           grid['minY']:grid['maxY'] + 1,
                                                           grid['minX']:grid['maxX'] + 1]

                    # elif "v1.0" in template_version and len(var_obj.dimensions) == 2:
                    #
                    #     if var_obj.dimensions[0] != "y" \
                    #        or var_obj.dimensions[1] != "x":
                    #         raise Exception("unexpected Geo2D variable")
                    #     var_obj[:] = in_nc.variables[name][grid['minY']:grid['maxY'] + 1,
                    #                                     grid['minX']:grid['maxX'] + 1]
                    elif "v1.1" in template_version and len(var_obj.dimensions) == 4:
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



def _get_comid_indices(find_comids, all_comids):

    if type(find_comids) is not numpy.ndarray:
        find_comids = numpy.array(find_comids)
    sorted_index = all_comids.argsort()
    sorted_all_comids = all_comids[sorted_index]
    found_index_sorted = numpy.searchsorted(sorted_all_comids, find_comids)
    index = sorted_index[found_index_sorted]
    return index


def subset_comid_file(in_nc_file=None, out_nc_file=None, comid_list=None,
                     template_version="v1.1", netcdf_format="NETCDF4_CLASSIC"):
    try:
        if not os.path.isfile(in_nc_file):
            raise Exception("in_nc_file missing @: {0}".format(in_nc_file))
        elif not os.path.isfile(out_nc_file):
            raise Exception("out_nc_file missing @: {0}".format(out_nc_file))

        comid_list_np = numpy.array(comid_list)
        comid_list_np.sort()

        with netCDF4.Dataset(in_nc_file, mode='r', format=netcdf_format) as in_nc:
            with netCDF4.Dataset(out_nc_file, mode='r+', format=netcdf_format) as out_nc:

                index_list = _get_comid_indices(comid_list_np, in_nc.variables['feature_id'][:])

                out_nc.model_initialization_time = in_nc.model_initialization_time
                out_nc.model_output_valid_time = in_nc.model_output_valid_time
                for name, var_obj in out_nc.variables.iteritems():
                    if len(var_obj.dimensions) == 1 and var_obj.dimensions[0] == "feature_id":
                        if name == "feature_id":
                            var_obj[:] = comid_list_np
                        else:
                            var_obj[:] = in_nc.variables[name][index_list]
                    else:
                        var_obj[:] = in_nc.variables[name][:]
    except Exception as ex:
        logger.exception(ex.message + in_nc_file)

