import pytest
from moto import mock_s3
import os
import boto3

from pathlib import Path

@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

@pytest.fixture
def s3_client(aws_credentials):
    with mock_s3():
        yield boto3.client('s3', region_name='us-east-1')

@pytest.fixture
def s3_resource(aws_credentials):
    with mock_s3():
        yield boto3.resource('s3', region_name='us-east-1')

@pytest.fixture
def default_environ():
    os.environ['DEFAULT_BUCKET'] = 'test-bucket' 
           
@pytest.fixture
def s3_test_bucket(s3_client, default_environ):
    bucket = os.environ['DEFAULT_BUCKET']
    s3_client.create_bucket(Bucket=bucket)
    yield s3_client

'------------------------------------fixture of tasks-----------------------------------------' 
@pytest.fixture()
def mock_etree(mocker):
    yield mocker.patch('dspaceq.tasks.tasks.etree.tostring')
    mocker.stopall()
    
@pytest.fixture()
def mock_get_requested_etds(mocker):
    yield mocker.patch('dspaceq.tasks.tasks.get_requested_etds')
    mocker.stopall()
    
@pytest.fixture()
def mock_boto3(mocker):
    yield mocker.patch('dspaceq.tasks.tasks.boto3')
    mocker.stopall()
    
@pytest.fixture()
def mock_mkdtemp(mocker):
    yield mocker.patch('dspaceq.tasks.tasks.mkdtemp')
    mocker.stopall()
    
@pytest.fixture()
def mock_check_call(mocker):
    yield mocker.patch('dspaceq.tasks.tasks.check_call')
    mocker.stopall()
@pytest.fixture()
def mock_rmtree(mocker):
    yield mocker.patch('dspaceq.tasks.tasks.rmtree')
    mocker.stopall()

@pytest.fixture()
def mock_mkdir(mocker):
    yield mocker.patch('dspaceq.tasks.tasks.mkdir')
    mocker.stopall()

@pytest.fixture()
def mock_get_mmsid(mocker):
    yield mocker.patch('dspaceq.tasks.tasks.get_mmsid')
    mocker.stopall()
    
@pytest.fixture()
def mock_list_s3_files(mocker):
    yield mocker.patch('dspaceq.tasks.tasks.list_s3_files')
    mocker.stopall()
    
@pytest.fixture(scope='function')
def mock_check_missing(mocker):
    yield mocker.patch('dspaceq.tasks.tasks.check_missing')
    mocker.stopall()

@pytest.fixture()
def mock_bib_metadata(mocker):
    'setup_mocks_for_metadata_transformations'
    yield (
        mocker.patch('dspaceq.tasks.tasks.get_bib_record'),
        mocker.patch('dspaceq.tasks.tasks.get_marc_from_bib'),
        mocker.patch('dspaceq.tasks.tasks.validate_marc'),
        mocker.patch('dspaceq.tasks.tasks.marc_xml_to_dc_xml'))
    mocker.stopall()

@pytest.fixture()
def mock_get_bib_record(mocker):
    mock_get_bib_record = mocker.patch('dspaceq.tasks.tasks.get_bib_record')
    mock_get_bib_record.return_value = open(str(Path(__file__).parent / "data/example_bib_record.xml"), "rb").read()
    yield mock_get_bib_record
    mocker.stopall()

@pytest.fixture()
def mock_guess_collection(mocker):
    yield mocker.patch('dspaceq.tasks.tasks.guess_collection')
    mocker.stopall()

@pytest.fixture()
def mock_celery_signature(mocker):
    yield mocker.patch('dspaceq.tasks.tasks.signature')
    mocker.stopall()

@pytest.fixture()
def mock_celery_group(mocker):
    yield  mocker.patch('dspaceq.tasks.tasks.group')
    mocker.stopall()
   
@pytest.fixture(scope='function')
def mock_get_digitized_bags(mocker):
    yield mocker.patch('dspaceq.tasks.tasks.get_digitized_bags')
    mocker.stopall()
    
'__________________________________________fixture of utils____________________________________________'
@pytest.fixture()
def mock_utils_get_bib_record(mocker):
    yield mocker.patch('dspaceq.tasks.utils.get_bib_record')
    mocker.stopall()
    
@pytest.fixture()
def mock_tasks_get_bib_record(mocker):
    yield mocker.patch('dspaceq.tasks.tasks.get_bib_record')
    mocker.stopall()

@pytest.fixture()
def mock_requests_get(mocker):
    yield mocker.patch('dspaceq.tasks.utils.requests.get')
    mocker.stopall()
    
@pytest.fixture()
def mock_celery_backend(mocker):
    yield mocker.patch('dspaceq.tasks.utils.Celery.backend')
    mocker.stopall()
'__________________________________________fixture of reports____________________________________________'

@pytest.fixture()
def mock_create_engine(mocker):
    yield mocker.patch('dspaceq.tasks.reports.create_engine')
    mocker.stopall()