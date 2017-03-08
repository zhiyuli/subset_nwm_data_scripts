import subprocess
import shutil
import netCDF4

def render_file(content_list=[], file_path=None):
    for item in content_list:
        replace_text_in_file(search_text=item[0], replace_text=item[1], file_path=file_path)


def replace_text_in_file(search_text=None, replace_text=None, file_path=None):
    sub = subprocess.call(['sed', '-i', 's/{0}/{1}/g'.format(search_text, replace_text), file_path])


def create_nc_from_cdf(cdl_file=None, out_file=None):
    # -7: netcdf-4 classic model
    sub = subprocess.call(['ncgen', '-7', '-o', out_file, cdl_file])

if __name__ == "__main__":

    grid = {}
    grid["minX"] = 837
    grid["maxX"] = 1328
    grid["minY"] = 1673
    grid["maxY"] = 2279
    dim_x_len = grid['maxX'] - grid['minX'] + 1
    dim_y_len = grid['maxY'] - grid['minY'] + 1

    # index_x_old = index_x_new + index_offset_x
    index_offset_x = grid["minX"]
    # index_y_old = index_y_new + index_offset_y
    index_offset_y = grid["minY"]


    template_file_original = "./nwm.t03z.forcing_analysis_assim.tm00.conus.cdl_template"
    template_file = template_file_original[0:template_file_original.rfind("_template")]
    nc_file = template_file_original.replace(".cdl_template", ".nc")
    shutil.copyfile(template_file_original, template_file)

    text_list = []
    text_list.append(["{%x%}", str(dim_x_len)])
    text_list.append(["{%y%}", str(dim_y_len)])
    text_list.append(["{%HH%}", "23"])
    text_list.append(["{%model_initialization_time%}", "2016-12-19_02:00:00"])
    text_list.append(["{%model_output_valid_time%}", "2016-12-19_03:00:00"])
    render_file(content_list=text_list, file_path=template_file)
    create_nc_from_cdf(cdl_file=template_file, out_file=nc_file)

    input_nc_file = '../nwm.t09z.forcing_analysis_assim.tm00.conus.nc'

    with netCDF4.Dataset(input_nc_file, mode='r', format="NETCDF4_CLASSIC") as input_nc:
        with netCDF4.Dataset(nc_file, mode='r+', format="NETCDF4_CLASSIC") as out_nc:

            for name, var_obj in out_nc.variables.iteritems():

                if name in ['x', 'y']:
                    var_x_or_y = var_obj
                    var_x_or_y[:] = input_nc.variables[name][grid['min{0}'.format(name.upper())]: \
                                                             grid['max{0}'.format(name.upper())] + 1]
                elif len(var_obj.dimensions) == 3:

                    if var_obj.dimensions[0] != "time" or var_obj.dimensions[1] != "y" \
                       or var_obj.dimensions[2] != "x":
                        raise Exception("unexpected Geo2D variable")
                    var_obj[0] = input_nc.variables[name][0,
                                                          grid['minY']:grid['maxY'] + 1,
                                                          grid['minX']:grid['maxX'] + 1]

                else:
                    if len(var_obj.dimensions) > 0:
                        var_obj[:] = input_nc.variables[name][:]





