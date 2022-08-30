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

from dspaceq.tasks.tasks import add, ingest_thesis_dissertation, dspace_ingest

from dspaceq.tasks.utils import FailedIngest

    
def test_add():
    assert add(21, 21) == 42


#@pytest.mark.skip(reason="not complete")
def test_dspace_ingest(tmpdir):
    mock_boto3 = patch('dspaceq.tasks.tasks.boto3').start()
    mock_mkdtemp = patch('dspaceq.tasks.tasks.mkdtemp', return_value=str(tmpdir)).start()
    mock_check_call = patch('dspaceq.tasks.tasks.check_call').start()
    mock_rmtree = patch('dspaceq.tasks.tasks.rmtree').start()
    mock_mkdir = patch('dspaceq.tasks.tasks.mkdir').start()

    # TODO: complete creation of temporary mapfile and populate test values
    mapfile = tmpdir / "mapfile"
    mapfile = Path(mapfile)
    mapfile.touch()
    with open(str(mapfile), 'w') as f:
        f.write('item_0 handle')
    assert Path.is_file(mapfile) == True
    
    item_dir = Path(tmpdir / 'item_0')
    item_dir.mkdir()
    
    # TODO: test of check_calls
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
    
def test_ingest_thesis_dissertation():
    mock_get_mmsid = patch('dspaceq.tasks.tasks.get_mmsid').start()
    mock_check_missing = patch('dspaceq.tasks.tasks.check_missing').start()
    mock_list_s3_files = patch('dspaceq.tasks.tasks.list_s3_files').start()
    mock_get_bib_record = patch('dspaceq.tasks.tasks.get_bib_record').start()
    mock_get_marc_from_bib = patch('dspaceq.tasks.tasks.get_marc_from_bib').start()
    mock_validate_marc = patch('dspaceq.tasks.tasks.validate_marc').start()
    mock_marc_xml_to_dc_xml = patch('dspaceq.tasks.tasks.marc_xml_to_dc_xml').start()
    mock_etree_tostring = patch('dspaceq.tasks.tasks.etree.tostring').start()
    mock_guess_collection = patch('dspaceq.tasks.tasks.guess_collection').start()
    mock_celery_signature = patch('dspaceq.tasks.tasks.signature').start()
    mock_celery_group = patch('dspaceq.tasks.tasks.group').start()

    mock_get_mmsid.return_value = '9876543210987'
    mock_check_missing.return_value = [(9876543210987, 'Test Error')]
    assert ingest_thesis_dissertation('Smith_2019_9876543210987') == {
        'Kicked off ingest': [], 'failed': {'Smith_2019_9876543210987': 'Missing required metadata in Alma - contact cataloging group'}
        }

    mock_list_s3_files.return_value = ['test.pdf', 'test.txt']
    mock_check_missing.return_value = [(9876543210987,[])]  # no missing metedata fields in Alma
    mock_etree_tostring.return_value = '<dc xmlns="http://www.loc.gov/MARC21/slim">test</dc>'
    mock_guess_collection.return_value = 'TEST thesis'

    assert ingest_thesis_dissertation('Smith_2019_9876543210987') == {'Kicked off ingest': ['Smith_2019_9876543210987'], 'failed': {}}
    assert ingest_thesis_dissertation('Smith_2019_9876543210987', 'TEST thesis') == {'Kicked off ingest': ['Smith_2019_9876543210987'], 'failed': {}}
        
def test_notify_etd_missing_fields():
    mock_get_requested_etds = patch('dspaceq.tasks.tasks.get_requested_etds').start()