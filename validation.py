import uuid
import os
import arcpy
from arcpy.sa import Raster


def netcdf2raster(inNetCDF_Path, variable, XDimension, YDimension, bandDimmension="",
                  dimensionValues="", valueSelectionMethod="BY_INDEX", outRaterPath=None):

    out_mem_raster_layer_name = str(uuid.uuid4())

    # Execute MakeNetCDFRasterLayer
    arcpy.MakeNetCDFRasterLayer_md(inNetCDF_Path, variable, XDimension, YDimension, out_mem_raster_layer_name, bandDimmension, dimensionValues, valueSelectionMethod)
    if outRaterPath is not None:
        if os.path.isfile(outRaterPath):
            os.remove(outRaterPath)
        arcpy.CopyRaster_management(out_mem_raster_layer_name,  outRaterPath, "", "", "", "NONE", "NONE", "")
    return out_mem_raster_layer_name


if __name__ == "__main__":

    try:
        inNetCDFFile = "F:\\subset_nwm_windows\\subset_nwm_data_scripts\\temp\\9470e277-5bbf-4aef-8467-eca991b28dde\\nwm.20170322\\analysis_assim\\nwm.tALLz.analysis_assim.land.tm00.conus.nc"
        variable = "ACCET"
        XDimension = "x"
        YDimension = "y"
        outRaterPath = "F:\\subset_nwm_windows\\subset_nwm_data_scripts\\temp\\sub.tif"

        dimensionValues = [["time", "10"]]
        raster_mem_sub = netcdf2raster(inNetCDFFile, variable, XDimension, YDimension, dimensionValues=dimensionValues)
        print raster_mem_sub

        inNetCDFFile = "G:\\nwm_new_data\\nwm.20170322\\analysis_assim\\nwm.t10z.analysis_assim.land.tm00.conus.nc"
        variable = "ACCET"
        XDimension = "x"
        YDimension = "y"
        outRaterPath = "F:\\subset_nwm_windows\\subset_nwm_data_scripts\\temp\\org.tif"

        raster_mem_org = netcdf2raster(inNetCDFFile, variable, XDimension, YDimension)
        print raster_mem_org

        arcpy.CheckOutExtension("Spatial")
        diff_raster = Raster(raster_mem_org) - Raster(raster_mem_sub)

        arcpy.CalculateStatistics_management(diff_raster)
        if diff_raster.maximum != 0 or diff_raster.minimum != 0:
            print inNetCDFFile
        print "Done"
        pass
    except Exception as ex:
        print str(ex)
        pass