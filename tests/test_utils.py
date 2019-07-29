import sys
from nose.tools import assert_true, assert_false, assert_equal, assert_not_equal, nottest
try:
    from unittest.mock import MagicMock, Mock, patch
except ImportError:
    from mock import MagicMock, Mock, patch
from requests.exceptions import HTTPError

from dspaceq.tasks.utils import get_mmsid


def test_get_mmsid_in_name():
    bag_name = 'Tyler_2019_9876543210987'
    response = get_mmsid(bag_name)
    assert_equal(response, '9876543210987')


def test_get_mmsid_not_in_name():
    bag_name = 'Tyler_2019'
    response = get_mmsid(bag_name)
    assert_not_equal(response, '9876543210987')