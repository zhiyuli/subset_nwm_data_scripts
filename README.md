# Subset National Water Model (NWM) NetCDF

Note that the deployment schedule of new National Water Model (NWM) Ver1.1 got delayed until May 4th 2017 according to its official notice: http://www.nws.noaa.gov/os/notification/scn17-41natl_water_model.htm

NWM's daily output is about 400GB worth of NetCDF files that contain meteorological and hydrologic data/forecasts covers the whole U.S.
This python library enables users to subset NWM Ver1.1 NetCDF using a polygon that represents a region of interest, which can significantly reduce data storage size and speed up regional data access.

More about NWM: http://water.noaa.gov/about/nwm

## Version naming convention:
"1.1.3", where "1.1" means NWM Ver1.1 and "3" is the version of this library.

## Workflow:
user-provided polygon --> Spatial Query module* --> stream comids & reservoir comids & grid cells (forcing & land & terrain) -->
Subset module --> Merge module --> regional NetCDF files

*: Spatial Query is optional if user directly provides stream comids & reservoir comids & grid cells

## Usage
See demo.py in source

The copy of the resulting NetCDF files of a huc_12 watershed (comid: 160102040504) for date 20170327 can be found at https://www.hydroshare.org/resource/734533a9e08c494aa28d2d0e688e2c06/

## What's new in 1.1.3 ?
1) Support subsetting more files:

1-1) "tm01" and "tm02" of each time stamp in analysis_assim model configuration

1-2) the "terrain" files in all model configurations

2) speed up spatial query on grid files (forcing, land and terrain) using GDAL and further reduce size of supporting files

3) add GDAL as a new dependency

## Environment and Dependencies:
Preliminary functional testings passed with Python 2.7.12 x64 on Windows 7 x64 and Ubuntu 16.04 x64.

### Spatial Query module:

1) fiona @ https://pypi.python.org/pypi/Fiona

2) shapely @ https://pypi.python.org/pypi/Shapely/

3) pysqlite with mod_spatialite extension @ https://pypi.python.org/pypi/pysqlite/ and https://www.gaia-gis.it/fossil/libspatialite/wiki?name=mod_spatialite

4) GDAL (gdal/ogr python bindings) @ https://pypi.python.org/pypi/GDAL

5) spatial query supporting files @ https://www.hydroshare.org/resource/e8434e287ada4540be814fa7275fb749/

### Subset & Merge module

1) NetCDF utilities (shell commands) @ https://www.unidata.ucar.edu/downloads/netcdf/index.jsp

2) NCO (shell commands) @ http://nco.sourceforge.net/

3) numpy @ https://pypi.python.org/pypi/numpy

4) netCDF4 python lib @ https://pypi.python.org/pypi/netCDF4

5) sed (shell command)

Implementation inspired by:

https://github.com/shawncrawley/subset_nwm_data_scripts

https://github.com/twhiteaker/pynwm