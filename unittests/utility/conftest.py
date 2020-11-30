import os
import pytest

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))


@pytest.fixture
def fixtures_dir():
    return os.path.join(TESTS_DIR, 'fixtures')
