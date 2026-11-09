import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.spark_session import get_spark  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    s = get_spark("tests", "local[2]")
    yield s
    s.stop()
