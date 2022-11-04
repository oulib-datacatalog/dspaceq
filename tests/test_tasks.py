from genericpath import isfile
import posix
#from subprocess import CalledProcessError
import sys
from unittest import TestCase
import os

from six import PY2

if PY2:
    from mock import MagicMock, Mock, patch
    from pathlib2 import Path
else:
    from unittest.mock import MagicMock, Mock, patch
    from pathlib import Path

import pytest
from requests.exceptions import HTTPError

from dspaceq.tasks.tasks import add, ingest_thesis_dissertation, dspace_ingest, notify_dspace_etd_loaded, list_missing_metadata_etd

from dspaceq.tasks.utils import FailedIngest

    
def test_add():
    assert add(21, 21) == 42
    
def test_dspace_ingest(tmpdir, mock_boto3, mock_mkdtemp, mock_check_call, mock_rmtree, mock_mkdir):
    mock_mkdtemp.return_value = str(tmpdir)
    mapfile = tmpdir / "mapfile"
    mapfile = Path(mapfile)
    mapfile.touch()
    with open(str(mapfile), 'w') as f:
        f.write('item_0 handle')
    assert Path.is_file(mapfile) == True
    
    item_dir = Path(tmpdir / 'item_0')
    item_dir.mkdir()
    
    bag_details = [{"bag name": {"files": ["committee.txt", "abstract.txt", "file.pdf"], "metadata": "xml", "metadata_ou": "ou.xml"}}]
    assert dspace_ingest(bag_details, collection="") == {"success":{'bag name': 'handle'}}
    
    assert Path.is_dir(item_dir) == True
    mock_rmtree.assert_called_with(str(tmpdir))
    assert mock_rmtree.call_count == 1
    
    mock_check_call.side_effect = FailedIngest('failed to ingest')
    with pytest.raises(FailedIngest) as call_error:
            dspace_ingest(bag_details, collection="")
    assert call_error.type == FailedIngest
    assert str(call_error.value) == 'failed to ingest'
    mock_boto3.resource.assert_called_with('s3')
    mock_rmtree.assert_called_with(str(tmpdir))
    assert mock_rmtree.call_count == 2
 
def test_ingest_thesis_dissertation(mock_get_mmsid, mock_list_s3_files, mock_check_missing, mock_bib_metadata, mock_etree, mock_guess_collection, mock_celery_signature, mock_celery_group):
    mock_get_mmsid.return_value = "9876543210987"
    mock_list_s3_files.return_value = ['test.pdf', 'test.txt']
    mock_bib_metadata[0].return_value = "9876543210987"
    mock_etree.return_value = '<dc xmlns="http://www.loc.gov/MARC21/slim">test</dc>'
    mock_guess_collection.return_value = 'TEST thesis'

    mock_check_missing.return_value = [(9876543210987, 'Test Error')]
    assert ingest_thesis_dissertation('Smith_2019_9876543210987') == {
        'Kicked off ingest': [], 'failed': {'Smith_2019_9876543210987': 'Missing required metadata in Alma - contact cataloging group'}
        }
    
    mock_check_missing.return_value = [(9876543210987,[])]
    assert ingest_thesis_dissertation('Smith_2019_9876543210987') == {'Kicked off ingest': ['Smith_2019_9876543210987'], 'failed': {}}
    assert ingest_thesis_dissertation('Smith_2019_9876543210987', 'TEST thesis') == {'Kicked off ingest': ['Smith_2019_9876543210987'], 'failed': {}}
        
def test_list_missing_metadata_etd(mock_get_mmsid, mock_check_missing, mock_get_digitized_bags,mock_get_requested_etds):
    mock_check_missing.return_value = [(9876543210987, 'Test Error')]
    assert list_missing_metadata_etd('Smith_2019_9876543210987') == [(9876543210987, 'Test Error')]
    mock_check_missing.return_value = [(9876543210987,[])]

    mock_get_requested_etds.return_value = []
    mock_get_digitized_bags.return_value = []
    mock_get_mmsid.return_value = "9876543210987"
    assert list_missing_metadata_etd('') == "No items found ready for ingest"
    mock_get_requested_etds.assert_called_with('.*')
        
    
def test_notify_dspace_etd_loaded():    
    arg = {'success': {}}
    assert notify_dspace_etd_loaded(arg) == "No items to ingest - no notification sent"
    
    arg = {'success': {'bagname': 'url'}}
    #assert notify_dspace_etd_loaded(arg) == "Ingest notification sent"

def test_ingest_thesis_dissertation_mock_s3(s3_resource, mock_get_mmsid, mock_check_missing, mock_get_bib_record, mock_celery_signature, mock_celery_group, s3_test_bucket):
    bucket = os.environ['DEFAULT_BUCKET']
    bag_name = "Smith_1819_12345678890123"

    mock_check_missing.return_value = [("1234567890123", [])]
    mock_get_bib_record.return_value = open(str(Path(__file__).parent / "data/example_bib_record.xml"), "rb").read()
    s3_test_bucket.put_object(Bucket=bucket, Key='private/shareok/{0}/data/committee.txt'.format(bag_name), Body='John Smith')
    s3_test_bucket.put_object(Bucket=bucket, Key='private/shareok/{0}/data/abstract.txt'.format(bag_name), Body='test abstract')
    
    #Test with good bag setting
    assert s3_resource.Object(bucket, 'private/shareok/{0}/data/committee.txt'.format(bag_name)).get()['Body'].read() == b'John Smith'
    assert ingest_thesis_dissertation(bag_name) == {'Kicked off ingest': [bag_name], 'failed': {}}
    
    #Test with invalid control character in abstract
    s3_test_bucket.put_object(Bucket=bucket, Key='private/shareok/{0}/data/abstract.txt'.format(bag_name), Body=open(str(Path(__file__).parent / "data/example_abstract_control_char.txt"), "rb").read())
    assert ingest_thesis_dissertation(bag_name) == {'Kicked off ingest': [], 'failed': {bag_name: 'Incompatible character found in abstract.txt'}}
    


    
    
    
    
