#import ez_setup
#ez_setup.use_setuptools()
from setuptools import setup, find_packages
setup(name='dspaceq',
      version='0.1.0',
      packages= find_packages(),
      package_data={'dspaceq':['tasks/xslt/*']},
      install_requires=[
          'celery==3.1.22',
          'pymongo==3.2.1',
          'requests==2.24.0',
          'sqlalchemy==1.2.12',
          'psycopg2==2.7.3.1',
          'jinja2',
          'boto3',
          'lxml',
      ],
      include_package_data=True,
)
