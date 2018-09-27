#import ez_setup
#ez_setup.use_setuptools()
from setuptools import setup, find_packages
setup(name='dspaceq',
      version='0.0.100',
      packages= find_packages(),
      package_data={'dspaceq':['tasks/templates/*.tmpl','dspaceq/tasks/templates/*.tmpl','tasks/xslt/*']},
      install_requires=[
          'celery==3.1.22',
          'pymongo==3.2.1',
          'requests==2.9.1',
          'xlrd==1.0.0',
          'xlwt==1.0.0',
          'pandas',
          'jinja2',
          'boto3',
          'lxml',
      ],
      include_package_data=True,
)
