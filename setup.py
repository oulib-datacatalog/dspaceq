#import ez_setup
#ez_setup.use_setuptools()
from setuptools import setup, find_packages
setup(name='dspaceq',      
      version='0.1.0',
      packages= find_packages(),
      package_data={'dspaceq':['tasks/xslt/*']},
      install_requires=[
          'celery',
          'pymongo',
          'requests==2.24.0',
          'sqlalchemy==1.3.20',
          'psycopg2==2.7.3.1',
          'jinja2',
          'boto3',
          'lxml',
      ],
      include_package_data=True,
)
