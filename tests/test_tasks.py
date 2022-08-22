import re
import sys

try:
    from unittest.mock import MagicMock, Mock, patch
except ImportError:
    from mock import MagicMock, Mock, patch

from requests.exceptions import HTTPError

from dspaceq.tasks.tasks import add, ingest_thesis_dissertation


def test_add():
    assert add(21, 21) == 42


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
    mock_check_missing.return_value = [(9876543210987,[])]
    mock_etree_tostring.return_value = '<dc xmlns="http://www.loc.gov/MARC21/slim">test</dc>'
    mock_guess_collection.return_value = 'TEST thesis'
    assert ingest_thesis_dissertation('Smith_2019_9876543210987') == {'Kicked off ingest': ['Smith_2019_9876543210987'], 'failed': {}}
    assert ingest_thesis_dissertation('Smith_2019_9876543210987', 'TEST thesis') == {'Kicked off ingest': ['Smith_2019_9876543210987'], 'failed': {}}