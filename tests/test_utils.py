# -*- coding: utf-8 -*-
import sys
import os
from bson.objectid import ObjectId
import pytest
from lxml import etree
from lxml.etree import XMLSyntaxError
import pkg_resources
import logging
from six import PY2, ensure_text
import boto3


if PY2:
    from mock import MagicMock, Mock, patch
    from pathlib2 import Path
else:
    from unittest.mock import MagicMock, Mock, patch
    from pathlib import Path

from requests.exceptions import HTTPError
import requests
from requests import codes, ConnectionError, ConnectTimeout

from dspaceq.tasks.utils import get_mmsid, get_bags, get_requested_mmsids, \
    get_requested_etds, get_bib_record, check_missing, missing_fields, get_digitized_bags, get_alma_url_field,\
    get_marc_from_bib, update_ingest_status, list_s3_files, chunk_list, guess_collection, marc_xml_to_dc_xml, validate_marc,\
    bib_to_dc
        

from bson.objectid import ObjectId

def test_get_mmsid_in_name():
    bag_name = 'Tyler_2019_9876543210987'
    response = get_mmsid(bag_name)
    assert response == '9876543210987'
    bag_name = 'Tyler_20191231_9876543210987'  # 8 digit date proceeding mmsid
    response = get_mmsid(bag_name)
    assert response == '9876543210987'
    bag_name = 'Tyler_20191231_9876543210987_ver2'  # 8 digit date proceeding mmsid
    response = get_mmsid(bag_name)
    assert response == '9876543210987'
    bag_name = '2019_Tyler_9876543210987'
    response = get_mmsid(bag_name)
    assert response == '9876543210987'
    bag_name = '2019_Tyler_9876543210987_ver2'
    response = get_mmsid(bag_name)
    assert response == '9876543210987'
    bag_name = '2019_Tyler_ver2_9876543210987'
    response = get_mmsid(bag_name)
    assert response == '9876543210987'
    bag_name = '20191231_Tyler_ver2_9876543210987'  # 8 digit date at beginning
    response = get_mmsid(bag_name)
    assert response == '9876543210987'

def test_get_mmsid_not_in_name():
    bag_name = 'Tyler_2019'
    response = get_mmsid(bag_name)
    assert response == None
    bag_name = '2019_Tyler'
    response = get_mmsid(bag_name)
    assert response == None
    bag_name = 'Tyler_2019_ver2'
    response = get_mmsid(bag_name)
    assert response == None
    bag_name = '2019_Tyler_ver2'
    response = get_mmsid(bag_name)
    assert response == None
    bag_name = '2019_Tyler_' + "9" * 7  # mmsid is between 8 and 19 digits
    response = get_mmsid(bag_name)
    assert response == None
    bag_name = '2019_Tyler_' + "9" * 20  # mmsid is between 8 and 19 digits
    response = get_mmsid(bag_name)
    assert response == None

def test_get_bags_no_next_url(mock_requests_get):
    result = """{
        "count": 0, 
        "meta": {
            "page": 1, 
            "page_size": 10, 
            "pages": 0
        }, 
        "next": null, 
        "previous": null, 
        "results": [1, 2, 3 ,4]
    }
    """
    mock_requests_get.return_value=Mock(ok=True, text=result)
    response = get_bags("test")
    assert list(response) == [1, 2, 3, 4]

def test_get_bags_with_next_url(mock_requests_get):
    result = """{
        "count": 0, 
        "meta": {
            "page": 1, 
            "page_size": 10, 
            "pages": 0
        }, 
        "next": "someurl", 
        "previous": null, 
        "results": [1, 2]
    }
    """
    mock_requests_get.return_value = Mock(ok=True)
    mock_requests_get.return_value.text = result
    response = get_bags("test")
    assert next(response) == 1
    assert next(response) == 2
    assert next(response) == 1
    assert next(response) == 2

def test_get_requested_mmsids(mock_celery_backend):
    result = [
      {"_id": ObjectId(b"123456789012"),
       "proquest_id": "",
       "call_number": "OU THESIS FLO",
       "name": "Requestor One",
       "title": "Some Title One",
       "creator": "Author One",
       "mmsid": "9876543210123",
       "year": "2019",
       "email": "requester@test.ou.edu",
       "other_identifiers": ""},
      {"_id": ObjectId(b"210987654321"),
       "proquest_id": "",
       "call_number": "OU THESIS BIB",
       "name": "Requestor Two",
       "title": "Some Title Two",
       "creator": "Author Two",
       "mmsid": "3210123456789",
       "year": "2013",
       "email": "requestor@test.ou.edu",
       "other_identifiers": ""}
    ]
    mock_celery_backend.database.client.catalog.etd.find.return_value = result
    response = get_requested_mmsids()
    assert response == ["9876543210123", "3210123456789"]

def test_get_requested_mmsids_no_results(mock_celery_backend):
    result = []
    mock_celery_backend.database.client.catalog.etd.find.return_value = result
    response = get_requested_mmsids()
    assert response == []

def test_get_requested_etds(mock_celery_backend):
    result = [
      {"_id": ObjectId(b"123456789012"),
       "proquest_id": "",
       "call_number": "OU THESIS FLO",
       "name": "Requestor One",
       "title": "Some Title One",
       "creator": "Author One",
       "mmsid": "9876543210123",
       "year": "2019",
       "email": "requester@test.ou.edu",
       "other_identifiers": ""}
    ]
    #mock_backend.database.client.catalog.etd.find.return_value = result
    mock_celery_backend.return_value.database.client.catalog.etd.find = result
    response = get_requested_etds("9876543210123")
    #assert response == result

def test_get_requested_etds_no_results(mock_celery_backend):
    result = []
    mmsid = None
    mock_celery_backend.database.client.catalog.etd.find.return_value = result
    response = get_requested_etds(mmsid)
    assert response == []

def test_get_bib_record(mock_requests_get):
    mock_requests_get.return_value = Mock(status_code=codes.OK, content="testing ascii")
    assert get_bib_record("placeholder_mmsid") == "testing ascii"
    mock_requests_get.return_value = Mock(status_code=codes.OK, content=u"testing ascii in unicode string")
    assert get_bib_record("placeholder_mmsid") == u"testing ascii in unicode string"
    mock_requests_get.return_value = Mock(status_code=codes.OK, content="testing unicode ☕ in ascii")
    assert get_bib_record("placeholder_mmsid") == "testing unicode ☕ in ascii"
    mock_requests_get.return_value = Mock(status_code=codes.OK, content=u"testing unicode ☕")
    assert get_bib_record("placeholder_mmsid") == u"testing unicode ☕"
    mock_requests_get.return_value = Mock(status_code=200, content="testing ascii")
    assert get_bib_record("placeholder_mmsid") == "testing ascii"
    mock_requests_get.return_value = Mock(status_code=200, content=u"testing ascii in unicode string")
    assert get_bib_record("placeholder_mmsid") == u"testing ascii in unicode string"
    mock_requests_get.return_value = Mock(status_code=200, content="testing unicode ☕ in ascii")
    assert get_bib_record("placeholder_mmsid") == "testing unicode ☕ in ascii"
    mock_requests_get.return_value = Mock(status_code=200, content=u"testing unicode ☕")
    assert get_bib_record("placeholder_mmsid") == u"testing unicode ☕"

def test_get_bib_record_not_ok_status(mock_requests_get):
    mock_requests_get.return_value = Mock(status_code=400)
    assert get_bib_record("placeholder_mmsid") == {"error": "Alma server returned code: 400"}
    mock_requests_get.return_value = Mock(status_code=403)
    assert get_bib_record("placeholder_mmsid") == {"error": "Alma server returned code: 403"}
    mock_requests_get.return_value = Mock(status_code=404)
    assert get_bib_record("placeholder_mmsid") == {"error": "Alma server returned code: 404"}
    mock_requests_get.return_value = Mock(status_code=500)
    assert get_bib_record("placeholder_mmsid") == {"error": "Alma server returned code: 500"}

def test_get_bib_record_connection_issues(mock_requests_get):
    mock_requests_get.side_effect = ConnectTimeout()
    assert get_bib_record("placeholder_mmsid") == {"error": "Alma Connection Error - try again later."}
    mock_requests_get.side_effect = ConnectionError()
    assert get_bib_record("placeholder_mmsid") == {"error": "Alma Connection Error - try again later."}  

@pytest.mark.parametrize("number", range(5))
def test_check_missing_with_missing_metadata(mock_utils_get_bib_record, number):
    mock_utils_get_bib_record.return_value = open(str(Path(__file__).parent / "data/example_bib_record.xml"), "rb").read()
    assert check_missing("99263190402042") == [('99263190402042', [ensure_text('502a: Thesis/Diss Tag'), ensure_text('690: School')])]

def test_missing_fields():
    """
    Test all the three cases of bib_record argument
    1. bib_record is None
    2. bib_record is not None but not dict
    3. bib_record is not None and is dict
    """
    assert missing_fields(None) == ["Could not find record!"]
    bib_record = {
        "245: Title": "record/datafield[@tag=245]"
    }
    assert list(missing_fields(bib_record)) == list(bib_record.values())
    bib_record = open(str(Path(__file__).parent / "data/example_bib_record.xml"), "rb").read()
    assert missing_fields(bib_record) == [ensure_text('502a: Thesis/Diss Tag'), ensure_text('690: School')]
    bib_record = open(str(Path(__file__).parent / "data/example_bib_record_with_url_field.xml"), "rb").read()
    assert missing_fields(bib_record) == [ensure_text('502a: Thesis/Diss Tag')]
    bib_record = open(str(Path(__file__).parent / "data/example_bib_record_with_502_sub_a.xml"), "rb").read()
    assert missing_fields(bib_record) == []
    bib_record = open(str(Path(__file__).parent / "data/missing_fields/example_author.xml"), "rb").read()
    assert missing_fields(bib_record) == [ensure_text('100: Author')]
    bib_record = open(str(Path(__file__).parent / "data/missing_fields/example_publish_year.xml"), "rb").read()
    assert missing_fields(bib_record) == [ensure_text('260/264: Publish Year')]
    bib_record = open(str(Path(__file__).parent / "data/missing_fields/example_school.xml"), "rb").read()
    assert missing_fields(bib_record) == [ensure_text('690: School')]
    bib_record = open(str(Path(__file__).parent / "data/missing_fields/example_subject.xml"), "rb").read()
    assert missing_fields(bib_record) == [ensure_text('600/610/611/630/650/651: Subject Heading')]
    bib_record = open(str(Path(__file__).parent / "data/missing_fields/example_thesis_dissertation.xml"), "rb").read()
    assert missing_fields(bib_record) == [ensure_text('502a: Thesis/Diss Tag')]
    bib_record = open(str(Path(__file__).parent / "data/missing_fields/example_title.xml"), "rb").read()
    assert missing_fields(bib_record) == [ensure_text('245: Title')]

def test_get_bib_record(mock_requests_get):
    mock_requests_get.return_value = Mock(status_code=200, content="testing ascii")
    assert get_bib_record('123') == 'testing ascii'
    
    mock_requests_get.side_effect = ConnectionError('Connection Error')
    assert get_bib_record('123') == {"error": "Alma Connection Error - try again later."}
    assert requests.codes.ok == 200

    mock_requests_get.side_effect = None
    mock_requests_get.return_value = Mock(status_code=404, content="testing ascii")
    assert get_bib_record('123') == {"error": "Alma server returned code: 404"}

def test_get_marc_from_bib():
    bib_record = open(str(Path(__file__).parent / "data/example_bib_record.xml"), "rb").read()
    record = open(str(Path(__file__).parent / "data/example_marc.xml"), "rb").read()
    assert etree.tostring(get_marc_from_bib(bib_record)) ==  record

def test_get_digitized_bags(mock_celery_backend):
    results = [{'bag':'shareok/bagname_123456789',
               'locations.s3.exists': True
              },
               {'bag':'shareok/digitized',
               'locations.s3.exists': True,
               }
              ]
    mock_celery_backend.database.client.catalog.digital_objects.find.return_value = results
    assert get_digitized_bags('123456789') == ['bagname_123456789', 'digitized']
    
def test_update_ingest_status(mock_celery_backend):
    mock_celery_backend.database.client.catalog.digital_objects.find_one.return_value = {'bag':'shareok/bagname', '_id': 'aaaaaaaaaaaaaaaaaaaaaaaa'}
    mock_celery_backend.database.client.catalog.digital_objects.update.return_value = {'nModified': 1}
    assert update_ingest_status('bagname','url', application='dspace', project=None, ingested=True) == None

def test_list_s3_files(s3_test_bucket):
    bucket = os.getenv('DEFAULT_BUCKET')
    bag = 'testbag'
    s3_test_bucket.put_object(Bucket=bucket, Key='private/shareok/{0}/data/image.txt'.format(bag), Body='test1')
    s3_test_bucket.put_object(Bucket=bucket, Key='private/shareok/{0}/data/image.pdf'.format(bag), Body='test2')
    assert sorted(list_s3_files(bag)) == ['private/shareok/testbag/data/image.pdf', 'private/shareok/testbag/data/image.txt']
    
def test_chunk_list():
    _list = [1,2,3,4,5,6,7,8,9,10]
    assert list(chunk_list(_list, 3)) == [[1,2,3], [4,5,6], [7,8,9], [10]]
    _list = ['filename', 1, 'bagname', 2, 'shareok', 3, 'test', 4, 'cybercom', 5]
    assert list(chunk_list(_list, 4)) == [['filename', 1, 'bagname', 2], ['shareok', 3, 'test', 4], ['cybercom', 5]]
    
def test_get_alma_url_field():
    bib_record = open(str(Path(__file__).parent / "data/example_bib_record_with_url_field.xml"), "rb").read()
    assert get_alma_url_field(bib_record) == 'https://shareok.org/11244/325437'
    
def test_guess_collection():
    bib_record = open(str(Path(__file__).parent / "data/example_bib_record_with_url_field.xml"), "rb").read()
    assert guess_collection(bib_record) == '11244/23528'
    
    bib_record = open(str(Path(__file__).parent / "data/example_bib_record_with_502_sub_a.xml"), "rb").read()
    assert guess_collection(bib_record) == '11244/10476'
    
def test_validate_marc():
    bib_record = open(str(Path(__file__).parent / "data/example_bib_record.xml"), "rb").read()
    record = open(str(Path(__file__).parent / "data/example_marc.xml"), "rb").read()
    bib_record_etree = etree.fromstring(bib_record)
    with pytest.raises(XMLSyntaxError):
        validate_marc(bib_record_etree)
    record_etree = etree.fromstring(record)
    assert etree.tostring(validate_marc(record_etree)) == record

def test_bib_to_dc():
    bib_record = open(str(Path(__file__).parent / "data/example_bib_record.xml"), "rb").read()
    record = open(str(Path(__file__).parent / "data/example_dc.xml"), "rb").read()
    assert bib_to_dc(bib_record) == record
    
def test_marc_xml_to_dc_xml():
    marc_record = open(str(Path(__file__).parent / "data/example_marc.xml"), "rb").read()
    record = open(str(Path(__file__).parent / "data/example_dc.xml"), "rb").read()
    assert etree.tostring(marc_xml_to_dc_xml(etree.fromstring(marc_record))) == record