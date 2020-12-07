import sys
import sqlalchemy
from sqlalchemy.dialects import postgresql
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
    now = datetime.now()
    engine_mock.return_value.connect.return_value.execute.return_value.fetchall.side_effect = [
        [["handle/1234", "item_id", now]],
        {AUTHOR: "Tyler",
         URI: "handle/1234",
         TITLE: "Reporting Test",
         ALTERNATIVE_TITLE: "How I learned to love testing",
         DEPARTMENT: "Info"}
    ]
    assert_equal(
        report_embargoed_items("2019-09-01", "2019-09-30"),
        [['handle/1234', 'Tyler', 'Reporting Test', 'Info', now.isoformat()]]
    )


def test_sqlalchemy_sql_template():
    template = "select * from :table;"
    result = "select * from %(table)s;"
    text = sqlalchemy.sql.text(template)
    compiled_text = str(text.compile(dialect=postgresql.dialect()))
    assert_equal(compiled_text, result)

