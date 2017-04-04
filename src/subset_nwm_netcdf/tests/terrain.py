import netCDF4
import numpy

# ncgen -7 -o nwm.t00z.analysis_assim.terrain_rt.tm00.conus_index.nc nwm.t00z.analysis_assim.terrain_rt.tm00.conus_index.cdl

# add x y index values to terrain file in order to create indexing polygon layer as land file did
in_nc_file = r"C:\nwm.t00z.analysis_assim.terrain_rt.tm00.conus.nc"
out_nc_file = r"C:\nwm.t00z.analysis_assim.terrain_rt.tm00.conus_index.nc"

x_len = 18432
y_len = 15360
try:

    with netCDF4.Dataset(in_nc_file, mode='r', format='NETCDF4_CLASSIC') as in_nc:
        with netCDF4.Dataset(out_nc_file, mode='r+', format='NETCDF4_CLASSIC') as out_nc:
            for name, var_obj in out_nc.variables.iteritems():
                if name in ['x', 'y']:
                    var_obj[:] = in_nc.variables[name][:]
                elif name == "x_index":
                    print "x_index"
                    var_obj[:] = numpy.array([range(x_len) for i in range(y_len)])
                elif name == "y_index":
                    print "y_index"
                    var_obj[:] = numpy.array([[i]*x_len for i in range(y_len)])
    print "Done"
    pass
except Exception as ex:
    print(ex.message)

