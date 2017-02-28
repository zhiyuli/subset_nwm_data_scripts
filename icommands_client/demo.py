from icommands import ICommands
import os
if __name__ == "__main__":

    # IRODS_HOST = 'hydrotest41.renci.org'
    # IRODS_PORT = '1247'
    # IRODS_DEFAULT_RESOURCE = 'hydrotest41Resc'
    # IRODS_ZONE = 'hydrotest41Zone'
    # IRODS_USERNAME = 'hsproxy'
    # IRODS_AUTH = 'proxywater1'

    IRODS_HOST = 'nwm.renci.org'
    IRODS_PORT = '1247'
    IRODS_DEFAULT_RESOURCE = ''
    IRODS_ZONE = 'nwmZone'
    IRODS_USERNAME = 'nwm-reader'
    IRODS_AUTH = 'nwmreader'

    icommands = ICommands()

    icommands.set_user_session(username=IRODS_USERNAME, password=IRODS_AUTH, host=IRODS_HOST,
                         port=IRODS_PORT, def_res=IRODS_DEFAULT_RESOURCE, zone=IRODS_ZONE,
                         userid=0, sess_id=None)

    #"(['analysis_assim', 'current', 'fe_analysis_assim',
    # 'fe_medium_range', 'fe_short_range', 'long_range',
    # 'medium_range', 'nomads', 'short_range', 'usgs_timeslices'], [])"
    base_irod_path = "/nwmZone/home/nwm/data/fe_short_range/20170221/"
    folder = icommands.listdir(base_irod_path)
    for item in folder[1]:
         print '"{0}",'.format(item)
    for item in folder[0]:
         print '"{0}",'.format(item)

    #exit(0)

    local_download_path="/media/sf_Shared_Folder/nwm/"
    if not os.path.exists(local_download_path):
        os.makedirs(local_download_path)

    file_list = [
        # "nwm.t00z.fe_short_range.f001.conus.nc_georeferenced.nc",
        # "nwm.t00z.fe_short_range.f002.conus.nc_georeferenced.nc",
        # "nwm.t00z.fe_short_range.f003.conus.nc_georeferenced.nc",
        "nwm.t00z.fe_short_range.f004.conus.nc_georeferenced.nc",
        "nwm.t00z.fe_short_range.f005.conus.nc_georeferenced.nc",
        "nwm.t00z.fe_short_range.f006.conus.nc_georeferenced.nc",
        "nwm.t00z.fe_short_range.f007.conus.nc_georeferenced.nc",
        "nwm.t00z.fe_short_range.f008.conus.nc_georeferenced.nc",
        "nwm.t00z.fe_short_range.f009.conus.nc_georeferenced.nc",
        "nwm.t00z.fe_short_range.f010.conus.nc_georeferenced.nc",
        "nwm.t00z.fe_short_range.f011.conus.nc_georeferenced.nc",
        "nwm.t00z.fe_short_range.f012.conus.nc_georeferenced.nc",
        "nwm.t00z.fe_short_range.f013.conus.nc_georeferenced.nc",
        "nwm.t00z.fe_short_range.f014.conus.nc_georeferenced.nc",
        "nwm.t00z.fe_short_range.f015.conus.nc_georeferenced.nc"
        ]

    for i in range(len(file_list)):

        file = file_list[i]
        file_irod_path = base_irod_path + file
        file_local_path = local_download_path + file
        print file_irod_path, file_local_path
        icommands.getFile(file_irod_path, file_local_path)
        print "Done (%s/%s)" % (str(i+1), len(file_list))

    icommands.delete_user_session()