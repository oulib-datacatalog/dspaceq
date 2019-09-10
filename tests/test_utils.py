# -*- coding: utf-8 -*-
import sys
from nose.tools import assert_true, assert_false, assert_equal, assert_not_equal, assert_is_none, nottest, assert_raises
try:
    from unittest.mock import MagicMock, Mock, patch
except ImportError:
    from mock import MagicMock, Mock, patch
from requests.exceptions import HTTPError
from requests import codes, ConnectionError, ConnectTimeout

from dspaceq.tasks.utils import get_mmsid, get_bags, get_requested_mmsids, \
    get_requested_etds, get_bib_record

from bson.objectid import ObjectId


def test_get_mmsid_in_name():
    bag_name = 'Tyler_2019_9876543210987'
    response = get_mmsid(bag_name)
    assert_equal(response, '9876543210987')
    bag_name = 'Tyler_20191231_9876543210987'  # 8 digit date proceeding mmsid
    response = get_mmsid(bag_name)
    assert_equal(response, '9876543210987')
    bag_name = 'Tyler_20191231_9876543210987_ver2'  # 8 digit date proceeding mmsid
    response = get_mmsid(bag_name)
    assert_equal(response, '9876543210987')
    bag_name = '2019_Tyler_9876543210987'
    response = get_mmsid(bag_name)
    assert_equal(response, '9876543210987')
    bag_name = '2019_Tyler_9876543210987_ver2'
    response = get_mmsid(bag_name)
    assert_equal(response, '9876543210987')
    bag_name = '2019_Tyler_ver2_9876543210987'
    response = get_mmsid(bag_name)
    assert_equal(response, '9876543210987')
    bag_name = '20191231_Tyler_ver2_9876543210987'  # 8 digit date at beginning
    response = get_mmsid(bag_name)
    assert_equal(response, '9876543210987')


def test_get_mmsid_not_in_name():
    bag_name = 'Tyler_2019'
    response = get_mmsid(bag_name)
    assert_is_none(response)
    bag_name = '2019_Tyler'
    response = get_mmsid(bag_name)
    assert_is_none(response)
    bag_name = 'Tyler_2019_ver2'
    response = get_mmsid(bag_name)
    assert_is_none(response)
    bag_name = '2019_Tyler_ver2'
    response = get_mmsid(bag_name)
    assert_is_none(response)
    bag_name = '2019_Tyler_' + "9" * 7  # mmsid is between 8 and 19 digits
    response = get_mmsid(bag_name)
    assert_is_none(response)
    bag_name = '2019_Tyler_' + "9" * 20  # mmsid is between 8 and 19 digits
    response = get_mmsid(bag_name)
    assert_is_none(response)


@patch("dspaceq.tasks.utils.requests.get")
def test_get_bags_no_next_url(mock_get):
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
    mock_get.return_value = Mock(ok=True)
    mock_get.return_value.text = result
    response = get_bags("test")
    assert_equal(list(response), [1, 2, 3, 4])


@patch("dspaceq.tasks.utils.requests.get")
def test_get_bags_with_next_url(mock_get):
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
    mock_get.return_value = Mock(ok=True)
    mock_get.return_value.text = result
    response = get_bags("test")
    assert_equal(next(response), 1)
    assert_equal(next(response), 2)
    assert_equal(next(response), 1)
    assert_equal(next(response), 2)


@patch("dspaceq.tasks.utils.Celery.backend")
def test_get_requested_mmsids(mock_backend):
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
    mock_backend.database.client.catalog.etd.find.return_value = result
    response = get_requested_mmsids()
    assert_equal(response, ["9876543210123", "3210123456789"])


@patch("dspaceq.tasks.utils.Celery.backend")
def test_get_requested_mmsids_no_results(mock_backend):
    result = []
    mock_backend.database.client.catalog.etd.find.return_value = result
    response = get_requested_mmsids()
    assert_equal(response, [])


@patch("dspaceq.tasks.utils.Celery.backend")
def test_get_requested_etds(mock_backend):
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
    mock_backend.database.client.catalog.etd.find.return_value = result
    response = get_requested_etds("9876543210123")
    assert_equal(response, result)


@patch("dspaceq.tasks.utils.Celery.backend")
def test_get_requested_etds_no_results(mock_backend):
    result = []
    mmsid = None
    mock_backend.database.client.catalog.etd.find.return_value = result
    response = get_requested_etds(mmsid)
    assert_equal(response, [])


@patch("dspaceq.tasks.utils.requests.get")
def test_get_bib_record(mock_get):
    mock_get.return_value = Mock(status_code=codes.OK, content="testing ascii")
    assert_equal(get_bib_record("placeholder_mmsid"), "testing ascii")
    mock_get.return_value = Mock(status_code=codes.OK, content=u"testing ascii in unicode string")
    assert_equal(get_bib_record("placeholder_mmsid"), u"testing ascii in unicode string")
    mock_get.return_value = Mock(status_code=codes.OK, content="testing unicode ☕ in ascii")
    assert_equal(get_bib_record("placeholder_mmsid"), "testing unicode ☕ in ascii")
    mock_get.return_value = Mock(status_code=codes.OK, content=u"testing unicode ☕")
    assert_equal(get_bib_record("placeholder_mmsid"), u"testing unicode ☕")
    mock_get.return_value = Mock(status_code=200, content="testing ascii")
    assert_equal(get_bib_record("placeholder_mmsid"), "testing ascii")
    mock_get.return_value = Mock(status_code=200, content=u"testing ascii in unicode string")
    assert_equal(get_bib_record("placeholder_mmsid"), u"testing ascii in unicode string")
    mock_get.return_value = Mock(status_code=200, content="testing unicode ☕ in ascii")
    assert_equal(get_bib_record("placeholder_mmsid"), "testing unicode ☕ in ascii")
    mock_get.return_value = Mock(status_code=200, content=u"testing unicode ☕")
    assert_equal(get_bib_record("placeholder_mmsid"), u"testing unicode ☕")


@patch("dspaceq.tasks.utils.requests.get")
def test_get_bib_record_not_ok_status(mock_get):
    mock_get.return_value = Mock(status_code=400)
    assert_equal(get_bib_record("placeholder_mmsid"), {"error": "Alma server returned code: 400"})
    mock_get.return_value = Mock(status_code=403)
    assert_equal(get_bib_record("placeholder_mmsid"), {"error": "Alma server returned code: 403"})
    mock_get.return_value = Mock(status_code=404)
    assert_equal(get_bib_record("placeholder_mmsid"), {"error": "Alma server returned code: 404"})
    mock_get.return_value = Mock(status_code=500)
    assert_equal(get_bib_record("placeholder_mmsid"), {"error": "Alma server returned code: 500"})


@patch("dspaceq.tasks.utils.requests.get")
def test_get_bib_record_connection_issues(mock_get):
    mock_get.side_effect = ConnectTimeout()
    assert_equal(get_bib_record("placeholder_mmsid"), {"error": "Alma Connection Error - try again later."})
    mock_get.side_effect = ConnectionError()
    assert_equal(get_bib_record("placeholder_mmsid"), {"error": "Alma Connection Error - try again later."})