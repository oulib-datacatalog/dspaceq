import sys
from nose.tools import assert_true, assert_false, assert_equal, nottest, assert_raises
try:
    from unittest.mock import MagicMock, Mock, patch, sentinel
except ImportError:
    from mock import MagicMock, Mock, patch, sentinel
from requests.exceptions import HTTPError

from dspaceq.tasks.reports import report_embargoed_items, AUTHOR, URI, TITLE, ALTERNATIVE_TITLE, DEPARTMENT
from datetime import datetime



@patch("dspaceq.tasks.reports.create_engine")
def test_report_embargoed_items_invalid_dates(engine_mock):
    engine_mock.Engine.connect.return_value = sentinel
    assert_equal(report_embargoed_items("2019-09-01", "2019"), {'ERROR': 'end_date does not use YYYY-MM-DD format'})
    assert_equal(report_embargoed_items("2019-09-01", "2019-09"), {'ERROR': 'end_date does not use YYYY-MM-DD format'})
    assert_equal(report_embargoed_items("2019-09-01", "2019/09/30"), {'ERROR': 'end_date does not use YYYY-MM-DD format'})
    assert_equal(report_embargoed_items("2019-09-01;inject", "2019-09-30"), {'ERROR': 'beg_date does not use YYYY-MM-DD format'})
    assert_raises(TypeError, report_embargoed_items, "2019-09-01")
    assert_raises(TypeError, report_embargoed_items, {"end_date": "2019-09-30"})


@patch("dspaceq.tasks.reports.create_engine")
def test_report_embargoed_items_valid_dates(engine_mock):
    engine_mock.Engine.connect.return_value = sentinel
    assert_equal(report_embargoed_items("2019-09-01", "2019-09-30"), [])


@patch("dspaceq.tasks.reports.create_engine")
def test_report_embargoed_items(engine_mock):
    #engine_mock.return_value.connect.return_value.execute.side_effect = side_effect
    #engine_mock.return_value.connect.return_value.execute.return_value.fetchall.return_value = [{"test": "test"}]
    engine_mock.return_value.connect.return_value.execute.return_value.fetchall.side_effect = [
        [{"handle": "test",
          "item_id": "test",
          "start_date": datetime.now()}],
        [{AUTHOR: "",
          URI: "",
          TITLE: "",
          ALTERNATIVE_TITLE: "",
          DEPARTMENT: ""}]
    ]
    print(report_embargoed_items("2019-09-01", "2019-09-30"))

test_report_embargoed_items()