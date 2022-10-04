import sys

try:
    from unittest.mock import MagicMock, Mock, patch, sentinel
except ImportError:
    from mock import MagicMock, Mock, patch, sentinel

import pytest
import sqlalchemy
from requests.exceptions import HTTPError
from sqlalchemy.dialects import postgresql

from dspaceq.tasks.reports import report_embargoed_items, AUTHOR, URI, TITLE, ALTERNATIVE_TITLE, DEPARTMENT
from datetime import datetime



def test_report_embargoed_items_invalid_dates(mock_create_engine):
    mock_create_engine.Engine.connect.return_value = sentinel
    assert report_embargoed_items("2019-09-01", "2019") == {'ERROR': 'end_date does not use YYYY-MM-DD format'}
    assert report_embargoed_items("2019-09-01", "2019-09") == {'ERROR': 'end_date does not use YYYY-MM-DD format'}
    assert report_embargoed_items("2019-09-01", "2019/09/30") == {'ERROR': 'end_date does not use YYYY-MM-DD format'}
    assert report_embargoed_items("2019-09-01;inject", "2019-09-30") == {'ERROR': 'beg_date does not use YYYY-MM-DD format'}
    with pytest.raises(TypeError):
        report_embargoed_items("2019-09-01")
        report_embargoed_items({"end_date": "2019-09-30"})

def test_report_embargoed_items_valid_dates(mock_create_engine):
    mock_create_engine.Engine.connect.return_value = sentinel
    assert report_embargoed_items("2019-09-01", "2019-09-30") == []


def test_report_embargoed_items(mock_create_engine):
    now = datetime.now()
    mock_create_engine.return_value.connect.return_value.execute.return_value.fetchall.side_effect = [
        [["handle/1234", "item_id", now]],
        {AUTHOR: "Tyler",
         URI: "handle/1234",
         TITLE: "Reporting Test",
         ALTERNATIVE_TITLE: "How I learned to love testing",
         DEPARTMENT: "Info"}
    ]
    assert report_embargoed_items("2019-09-01", "2019-09-30") == [['handle/1234', 'Tyler', 'Reporting Test', 'Info', now.isoformat()]]

def test_sqlalchemy_sql_template():
    template = "select * from :table;"
    result = "select * from %(table)s;"
    text = sqlalchemy.sql.text(template)
    compiled_text = str(text.compile(dialect=postgresql.dialect()))
    assert compiled_text == result

