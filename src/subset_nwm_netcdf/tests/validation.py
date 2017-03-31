import uuid
import os
import arcpy
from arcpy.sa import Raster
import copy
import netCDF4
import numpy

arcpy.CheckOutExtension("Spatial")


def netcdf2raster(inNetCDF_Path, variable, XDimension, YDimension, bandDimmension="",
                  dimensionValues="", valueSelectionMethod="BY_INDEX", outRaterPath=None):

    out_mem_raster_layer_name = str(uuid.uuid4())

    # Execute MakeNetCDFRasterLayer
    arcpy.MakeNetCDFRasterLayer_md(inNetCDF_Path, variable, XDimension, YDimension, out_mem_raster_layer_name,
                                   bandDimmension, dimensionValues, valueSelectionMethod)
    if outRaterPath is not None:
        if os.path.isfile(outRaterPath):
            os.remove(outRaterPath)
        arcpy.CopyRaster_management(out_mem_raster_layer_name,  outRaterPath, "", "", "", "NONE", "NONE", "")
    return Raster(out_mem_raster_layer_name)


def validate_netcdf_geo2d(inNetCDFFile_small=None, inNetCDFFile_big=None, variable=None,
                          XDimension="x", YDimension="y", dimensionValues=None):

    raster_small = netcdf2raster(inNetCDFFile_small, variable, XDimension, YDimension, dimensionValues=dimensionValues)
    raster_big = netcdf2raster(inNetCDFFile_big, variable, XDimension, YDimension)

    diff_raster = raster_big - raster_small
    arcpy.CalculateStatistics_management(diff_raster)
    if diff_raster.maximum != 0 or diff_raster.minimum != 0:
        print "Data does not match @ {0}".format(inNetCDFFile_small)
    else:
        print "validation passed"
    pass


def validate_grid_file(big_nc_file=None, small_nc_file=None, grid_dict=None,
                       template_version="v1.1", netcdf_format="NETCDF4_CLASSIC"):
    try:
        if not os.path.isfile(big_nc_file):
            raise Exception("big_nc_file missing @: {0}".format(big_nc_file))
        elif not os.path.isfile(small_nc_file):
            raise Exception("small_nc_file missing @: {0}".format(small_nc_file))

        grid = copy.copy(grid_dict)
        with netCDF4.Dataset(big_nc_file, mode='r', format=netcdf_format) as big_nc:
            with netCDF4.Dataset(small_nc_file, mode='r', format=netcdf_format) as small_nc:

                assert(big_nc.model_initialization_time == small_nc.model_initialization_time)
                assert (big_nc.model_initialization_time == small_nc.model_initialization_time)
                assert (big_nc.model_output_valid_time == small_nc.model_output_valid_time)
                for name, var_obj in small_nc.variables.iteritems():

                    if name in ['x', 'y']:
                        var_x_or_y = var_obj
                        assert (var_x_or_y[:] == big_nc.variables[name][grid['min{0}'.format(name.upper())]:
                                                               grid['max{0}'.format(name.upper())] + 1])

                    elif len(var_obj.dimensions) == 2:
                        if var_obj.dimensions[0] != "time":
                            raise Exception("unexpected variable")
                        assert (var_obj[0][:] == big_nc.variables[name][:])

                    elif len(var_obj.dimensions) == 3:

                        if var_obj.dimensions[0] != "time" or var_obj.dimensions[1] != "y" \
                           or var_obj.dimensions[2] != "x":
                            raise Exception("unexpected Geo2D variable")
                        assert (numpy.all((var_obj[0] == big_nc.variables[name][0,
                                                           grid['minY']:grid['maxY'] + 1,
                                                           grid['minX']:grid['maxX'] + 1])==True))
                    elif len(var_obj.dimensions) == 4:
                        # medium range land file variable may have 4 dimensions

                        if var_obj.dimensions[0] != "time" or var_obj.dimensions[1] != "y" \
                           or var_obj.dimensions[3] != "x":
                            raise Exception("unexpected medium range Geo2D variable")
                        assert (var_obj[0] == big_nc.variables[name][0,
                                                           grid['minY']:grid['maxY'] + 1,
                                                           :,
                                                           grid['minX']:grid['maxX'] + 1])
                    else:
                        if len(var_obj.dimensions) > 0:
                            assert (var_obj[:] == big_nc.variables[name][:])
    except Exception as ex:
       print (ex.message + small_nc)


if __name__ == "__main__":

    try:
        inNetCDFFile_small = r"C:\demo_subset_nwm_netcdf\temp\0128a466-443a-41f6-8f17-b6b98a500dd1\nwm.20170328\analysis_assim\nwm.tALLz.analysis_assim.land.tm00.conus.nc"
        variable = "SNOWT_AVG"
        XDimension = "x"
        YDimension = "y"
        dimensionValues = [["time", "1"]]
        inNetCDFFile_big = r"G:\nwm_new_data\\nwm.20170327\\analysis_assim\\nwm.t01z.analysis_assim.land.tm00.conus.nc"

        validate_netcdf_geo2d(inNetCDFFile_big=inNetCDFFile_big,
                              inNetCDFFile_small=inNetCDFFile_small,
                              XDimension=XDimension,
                              YDimension=YDimension,
                              variable=variable,
                              dimensionValues=dimensionValues)
        # grid_dict = {'minX': 1069, 'minY': 2184, 'maxX': 1103, 'maxY': 2219}
        # validate_grid_file(big_nc_file=inNetCDFFile_big, small_nc_file=inNetCDFFile_small, grid_dict=grid_dict,)

    except Exception as ex:
        print ex.messag
