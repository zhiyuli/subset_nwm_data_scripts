import sys
from osgeo import osr
import re

def esriprj2standards(shapeprj_path):
   prj_file = open(shapeprj_path, 'r')
   prj_txt = prj_file.read()
   srs = osr.SpatialReference()
   srs.ImportFromESRI([prj_txt])
   print 'Shape prj is: %s' % prj_txt
   print 'WKT is: %s' % srs.ExportToWkt()
   print 'Proj4 is: %s' % srs.ExportToProj4()
   srs.AutoIdentifyEPSG()
   print 'EPSG is: %s' % srs.GetAuthorityCode(None)

#esriprj2standards(sys.argv[1])
# esriprj2standards("/home/drew/Desktop/state/state_local.prj")


def wkt2epsg(wkt, epsg='./epsg', forceProj4=False):
   ''' Transform a WKT string to an EPSG code

   Arguments
   ---------

   wkt: WKT definition
   epsg: the proj.4 epsg file (defaults to '/usr/local/share/proj/epsg')
   forceProj4: whether to perform brute force proj4 epsg file check (last resort)

   Returns: EPSG code

   '''

   code = None
   p_in = osr.SpatialReference()
   s = p_in.ImportFromWkt(wkt)
   if s == 5:  # invalid WKT
      return None
   if p_in.IsLocal() == 1:  # this is a local definition
      return p_in.ExportToWkt()
   if p_in.IsGeographic() == 1:  # this is a geographic srs
      cstype = 'GEOGCS'
   else:  # this is a projected srs
      cstype = 'PROJCS'
   an = p_in.GetAuthorityName(cstype)
   ac = p_in.GetAuthorityCode(cstype)
   if an is not None and ac is not None:  # return the EPSG code
      return '%s:%s' % \
             (p_in.GetAuthorityName(cstype), p_in.GetAuthorityCode(cstype))
   else:  # try brute force approach by grokking proj epsg definition file
      p_out = p_in.ExportToProj4()
      if p_out:
         if forceProj4 is True:
            return p_out
         f = open(epsg)
         for line in f:
            if line.find(p_out) != -1:
               m = re.search('<(\\d+)>', line)
               if m:
                  code = m.group(1)
                  break
         if code:  # match
            return 'EPSG:%s' % code
         else:  # no match
            return None
      else:
         return None

#print wkt2epsg(wkt='PROJCS["NAD_1983_Wisconsin_TM_US_Ft",GEOGCS["GCS_North_American_1983",DATUM["D_North_American_1983",SPHEROID["GRS_1980",6378137.0,298.257222101]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Transverse_Mercator"],PARAMETER["False_Easting",1706033.333333333],PARAMETER["False_Northing",-14698133.33333333],PARAMETER["Central_Meridian",-90.0],PARAMETER["Scale_Factor",0.9996],PARAMETER["Latitude_Of_Origin",0.0],UNIT["Foot_US",0.3048006096012192]]')

print wkt2epsg(wkt='GEOGCS["GCS_North_American_1983",DATUM["D_North_American_1983",SPHEROID["GRS_1980",6378137.0,298.257222101]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]]')
