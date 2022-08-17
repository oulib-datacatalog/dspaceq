import sys

try:
    from unittest.mock import MagicMock, Mock, patch
except ImportError:
    from mock import MagicMock, Mock, patch

from requests.exceptions import HTTPError

from dspaceq.tasks.tasks import add, ingest_thesis_dissertation


def test_add():
    assert add(21, 21) == 42


