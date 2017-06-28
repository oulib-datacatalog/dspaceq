#import ez_setup
#ez_setup.use_setuptools()
from setuptools import setup, find_packages
setup(name='dspaceq',
      version='0.0',
      packages= find_packages(),
      package_data={'dspaceq':['tasks/templates/*.tmpl','dspaceq/tasks/templates/*.tmpl']},
      install_requires=[
          'celery==3.1.22',
          'pymongo==3.2.1',
          'requests==2.9.1',
          'pandas',
          'jinja2',
      ],
      include_package_data=True,
)
