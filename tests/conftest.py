import pytest
from assemply import QueueNode


@pytest.fixture
def queue():
    return QueueNode()

