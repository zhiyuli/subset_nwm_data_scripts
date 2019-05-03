from setuptools import setup, find_packages

setup(name='subset_nwm_netcdf',
      version='2.0.0',
      description='Subset National Water Model (NWM) NetCDF',
      long_description=open("README.rst").read(),
      long_description_content_type="text/markdown",
      url='https://github.com/zhiyuli/subset_nwm_netcdf',
      author='Zhiyu/Drew Li',
      author_email='zyli2004@gmail.com',
      license='BSD-2',
      include_package_data=True,
      packages=find_packages('src'),
      package_dir={'': 'src'},
      zip_safe=False,
      install_requires=[
          'Fiona>=1.7.5',
          'Shapely>=1.5.17',
          'numpy>=1.2.1',
          'netCDF4>=1.2.7',
          'GDAL>=2.1.2'
          # also need nco bin, netcdf4 bin
          # python2 needs pyspatialite or pysqlite2
      ]
      )
