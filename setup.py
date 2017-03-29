from setuptools import setup, find_packages

setup(name='subset_nwm_netcdf',
      version='1.0',
      description='Subset NWM netcdf ver1.1',
      url='https://github.com/zhiyuli/subset_nwm_netcdf',
      author='Zhiyu/Drew Li',
      author_email='zyli2004@gmail.com',
      license='MIT',
      include_package_data=True,
      packages=find_packages('src'),
      package_dir={'': 'src'},
      package_data={'': ['*.py', '*.md'],
                    'subset_nwm_netcdf': ['static/sed_win/*',
                                          'static/data/utah/*',
                                          'static/netcdf_templates/v1.1/forcing/analysis_assim/*',
                                          'static/netcdf_templates/v1.1/forcing/medium_range/*',
                                          'static/netcdf_templates/v1.1/forcing/short_range/*',
                                          'static/netcdf_templates/v1.1/forecast/analysis_assim/*',
                                          'static/netcdf_templates/v1.1/forecast/short_range/*',
                                          'static/netcdf_templates/v1.1/forecast/medium_range/*',
                                          'static/netcdf_templates/v1.1/forecast/long_range/*'],
                    },
      zip_safe=False,
      )
