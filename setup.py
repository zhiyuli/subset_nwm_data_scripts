from setuptools import setup, find_packages

setup(name='subset_nwm_netcdf',
      version='1.2.1',
      description='Subset NWM netcdf ver1.1',
      url='https://github.com/zhiyuli/subset_nwm_netcdf',
      author='Zhiyu/Drew Li',
      author_email='zyli2004@gmail.com',
      license='MIT',
      include_package_data=True,
      packages=find_packages('src'),
      package_dir={'': 'src'},
      zip_safe=False,
      )
