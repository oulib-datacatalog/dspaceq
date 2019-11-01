import sys
from nose.tools import assert_true, assert_false, assert_equal, nottest
try:
    from unittest.mock import MagicMock, Mock, patch
except ImportError:
    from mock import MagicMock, Mock, patch
from requests.exceptions import HTTPError

from dspaceq.tasks.tasks import add


def test_add():
    assert_equal(add(21, 21), 42)


