import netCDF4
import numpy


# 1) Run this script to add cell indices (x_index, y_index) to forcing/land and terrain files as two new variables.
# 2) ArcMap Make NetCDF Layer to convert the two variables (x_index and y_index) into GeoTiff rasters.
# 3) ArcMap Composite Raster Band to merge x_index raster and y_index raster into one 2-band raster
# 4) ArcMap Re-project raster into NAD1983 (epsg 4269)
# 5) Export raster as Tiff file
# 6) "Copy Raster" to change cell value type to 16bit-unsigned, set NoDataValue to 65535, use LZZ compression
# 7) Python-GDAL to cut those two raster using a polygon, and then calculate min and max values of resulting rasters.


# ## ncgen -7 -o nwm.t00z.analysis_assim.terrain_rt.tm00.conus_index.nc nwm.t00z.analysis_assim.terrain_rt.tm00.conus_index.cdl
# in_nc_file = "../static/data/tests/nwm.t00z.analysis_assim.terrain_rt.tm00.conus.nc"
# out_nc_file = "../static/data/tests/nwm.t00z.analysis_assim.terrain_rt.tm00.conus_xy.nc"
# x_len = 18432
# y_len = 15360


## ncgen -7 -o nwm.t00z.analysis_assim.land.tm00.conus_index.nc nwm.t00z.analysis_assim.land.tm00.conus_index.cdl
in_nc_file = "../static/data/tests/nwm.t00z.analysis_assim.land.tm00.conus.nc"
out_nc_file = "../static/data/tests/nwm.t00z.analysis_assim.land.tm00.conus_xy.nc"
x_len = 4608
y_len = 3840


try:
    with netCDF4.Dataset(in_nc_file, mode='r', format='NETCDF4_CLASSIC') as in_nc:
        with netCDF4.Dataset(out_nc_file, mode='r+', format='NETCDF4_CLASSIC') as out_nc:
            for name, var_obj in out_nc.variables.iteritems():
                if name in ['x', 'y']:
                    var_obj[:] = in_nc.variables[name][:]
                # elif name == "x_index":
                #     print "x_index"
                #     var_obj[:] = numpy.array([range(x_len) for i in range(y_len)])
                # elif name == "y_index":
                #     print "y_index"
                #     var_obj[:] = numpy.array([[i]*x_len for i in range(y_len)])
    print "Done"
    pass
except Exception as ex:
    print(ex.message)
