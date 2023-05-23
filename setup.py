#import ez_setup
#ez_setup.use_setuptools()
from setuptools import setup, find_packages
setup(name='dspaceq',      
      version='0.2.2',
      packages= find_packages(),
      package_data={'dspaceq':['tasks/xslt/*']},
      install_requires=[
          'celery==5.2.7 ; python_version >= "3.6"',
          'celery==3.1.22 ; python_version < "3.6"',
          'pymongo==3.12.3 ; python_version >= "3.6"',
          'pymongo==3.2.1 ; python_version < "3.6"',
          'requests==2.31.0',
          'sqlalchemy==1.4.42',
          'psycopg2-binary==2.9.3 ; python_version >= "3.6"',
          'psycopg2-binary==2.8.6 ; python_version < "3.6"',
          'jinja2',
          'boto3',
          'lxml',
          'six',
      ],
      include_package_data=True,
)
