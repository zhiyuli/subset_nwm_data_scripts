# Subset National Water Model (NWM) NetCDF

National Water Model (NWM) Ver1.1 has been in production since UTC time 12PM May 8, 2017

Details about changes in v1.1:  http://www.nws.noaa.gov/os/notification/scn17-41natl_water_modelaaa.htm

NWM's daily output is about 430GB worth of NetCDF files that contain meteorological and hydrologic data/forecasts covers the whole U.S.
This python library enables users to subset NWM Ver1.1 NetCDF using a polygon that represents a region of interest, which can significantly reduce data storage size and speed up regional data access.

More about NWM: http://water.noaa.gov/about/nwm

## Version naming convention:
"1.1.X", say "1.1.6", where "1.1" means NWM Ver1.1 and "6" is the version of this library.

## Workflow:
user-provided polygon --> Spatial Query module* --> stream comids & reservoir comids & grid cell indices (forcing & land & terrain) -->
Subset module --> Merge module --> regional NetCDF files

*: Spatial Query is optional if user directly provides stream comids & reservoir comids & grid cells indices

## Usage
See demo.py in source

The copy of the resulting NetCDF files of TwoMileCreek watershed can be found at https://www.hydroshare.org/resource/fa9af1222795490a953292def5852ace/

The watershed poylgon shapefile is at /www.hydroshare.org/resource/9d0e4cab63d74c0b8e6b6d83254c30de/

## What's new in 1.1.9 ?
This version has one minor bug fix for National Water Model Viewer Tethys App.
App can subset AA data back to 2017-05-09 (the first archived full-day data for NWM Ver1.1)

## What's new in 1.1.8 ?

 1) Improve coordinate projection handling:
    If shapefile/geojson/wkt user provides has a custom projection that SQLite DB doesn't support, use GDAL to re-project it to a well-know projection (4326) before spatial query

 2) Merge multi-day analysis_assim subsetting results into one:
    The naming converstion of resulting files:
    nwm.tALLz.analysis_assim.{geometry}.tm{tm}.conus.nc
    {geometry}: forcing, channel_rt, reservoir, terrain_rt, land
    {tm}: 00, 01, 02
    Daily analysis_assim subsetting results will still be kept

## What's new in 1.1.7 ?

 1) Add a new flag "include_AA_tm12" to control whether to subset tm01 and tm02 files of analysis_assim configuration. Default is True

 2) fix a bug in spatial query module: If incoming geometry is 3D, convert it to 2D before spatial query

## What's new in 1.1.6 ?

 1) Remove previously added "time" dimension in "reference_time" variable as it was a violation of CF convention on "coordinate variable"";
 see: http://www.unidata.ucar.edu/software/netcdf/workshops/2011/datamodels/NcCVars.html
 But this change causes missing "reference_time" value in merged netcdfs as it only stores the value from the first file;

 2) Fix bug in "time_bounds" variable;

 3) Add "pyspatialite" as a optional dependency in spatial query. Will try loading pysqlite2 by default, if failed,  try loading pyspatialite lib.
 Note: Installation of pyspatialite needs several manual steps on linux, but it is so far a good way for CentOS. On Ubuntu and Windows, user should install pysqlite2 + mod_spatialite binary;

## What's new in 1.1.5 ?

 1) Add two new flags, one for 2D grid file (forcing/land/terrain) and one for 1D file (channel/reservoir), to specify whether to keep original dimension size unchanged in resulting outputs.

 1-1) If True, the sizes of dimension 'x' and 'y' for 2D grid resulting file and 'feature_id' for 1D resulting file will be same as their originals;
 Variables outside subsetting domain will be set to corresponding Missing Data Values.

 1-2) If False, the size of above dimension will be shrunk to cover subsetting domain.

 Note: keep original dimension unchanged will slightly increase output size for both 1D and 2D files and significantly slow down subsetting process for 1D file.

 2) Add python dependencies to setup.py file.

## What's new in 1.1.4 ?

 Use a new approach to perform spatial query on grid cell indices against forcing, land and terrain files;
 Further improve spatial query speed on grid cells;
 Further address partially covered grids caused by projection distortion;
 Deprecate Tiff supporting files. Now the only necessary supporting file is the sqlite/spatialite geodatabase for stream, reservoir and HUCs;
 Remove GDAL; Add pyproj and numpy to dependency list;

## What's new in 1.1.3 ?
1) Support subsetting more files:

1-1) "tm01" and "tm02" of each time stamp in analysis_assim model configuration

1-2) the "terrain" files in all model configurations

2) speed up spatial query on grid files (forcing, land and terrain) using GDAL and further reduce size of supporting files

3) add GDAL as a new dependency

## What's new in 1.1.2 and 1.1.1?

First two releases

## Environment and Dependencies:
Preliminary functional testings passed with Python 2.7.12 x64 on Windows 7 x64 and Ubuntu 16.04 x64.

### Spatial Query module:

1) fiona >= 1.7.5 @ https://pypi.python.org/pypi/Fiona

2) shapely >= 1.5.17 @ https://pypi.python.org/pypi/Shapely/

3) pysqlite >= 2.8.3 with mod_spatialite extension @ https://pypi.python.org/pypi/pysqlite/ and https://www.gaia-gis.it/fossil/libspatialite/wiki?name=mod_spatialite

4) numpy >= 1.12.1 @ https://pypi.python.org/pypi/numpy

5) GDAL >= 2.1.3 @ https://pypi.python.org/pypi/GDAL/2.1.3

6) spatial query supporting files (1.1.4) @ https://www.hydroshare.org/resource/23c05d3177654a9ab9dc9023d00d16ed/

### Subset & Merge module

1) NetCDF utilities >= 4.4 (shell commands) @ https://www.unidata.ucar.edu/downloads/netcdf/index.jsp

2) NCO >= 4.6.3 (shell commands) @ http://nco.sourceforge.net/

3) numpy >= 1.12.1 @ https://pypi.python.org/pypi/numpy

4) netCDF4 >= 1.2.7 python wrapper for NetCDF4 @ https://pypi.python.org/pypi/netCDF4

5) sed (shell command): Linux systems include this command by default. This lib contains a GNU sed binary for Windows

Implementation inspired by:

https://github.com/shawncrawley/subset_nwm_data_scripts

https://github.com/twhiteaker/pynwm