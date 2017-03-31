# subset_nwm_netcdf

The National Water Model (NWM) is supposed to produce forecasts in a new format (Ver1.1) starting on April 1st, 2017.
Its daily output is about 400GB worth of netcdf files that contain hydrologic data/forecasts for the whole U.S.
This python library enables users to subset NWM Ver1.1 NetCDFs using a polygon, which can significantly reduce
data size and let researchers quickly access/store regional data they are interested in.

#Environment and Dependencies:

Preliminary functional testings passed in Python 2.7.12 x64 on Windows 7 x64 and Ubuntu 16.04 x64.

Query module:

1) fiona @ https://pypi.python.org/pypi/Fiona

2) shapely @ https://pypi.python.org/pypi/Shapely/

3) pysqlite with mod_spatialite extension @ https://pypi.python.org/pypi/pysqlite/ and https://www.gaia-gis.it/fossil/libspatialite/wiki?name=mod_spatialite

4) NWM sqlite geodatabase (4.5GB split zip files) @ https://www.hydroshare.org/resource/95410260015a4fd1858a3ad3c4aa7f17/

Subset & Merge module

1) NetCDF utilities (shell commands) @ https://www.unidata.ucar.edu/downloads/netcdf/index.jsp

2) NCO (shell commands) @ http://nco.sourceforge.net/

3) numpy @ https://pypi.python.org/pypi/numpy

4) netCDF4 python lib @ https://pypi.python.org/pypi/netCDF4

5) sed (shell command)


More about NWM: http://water.noaa.gov/about/nwm

Implementation inspired by:

https://github.com/shawncrawley/subset_nwm_data_scripts

https://github.com/twhiteaker/pynwm