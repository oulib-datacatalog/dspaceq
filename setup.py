#import ez_setup
#ez_setup.use_setuptools()
from setuptools import setup, find_packages
setup(name='dspaceq',      
      version='0.2.1f',
      packages= find_packages(),
      package_data={'dspaceq':['tasks/xslt/*']},
      install_requires=[
          'celery==3.1.22',
          'pymongo==3.2.1',
          'requests==2.24.0',
          'sqlalchemy==1.3.20',
          'psycopg2==2.7.3.1',
          'jinja2',
          'boto3',
          'lxml',
      ],
      include_package_data=True,
)
