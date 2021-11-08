import pytest

from .utils import process

TABLES = [
    "Bookings",
    "Contacts",
]
START = "2021-10-01"
END = "2021-11-09"


@pytest.mark.parametrize("table", TABLES)
def test_auto(table):
    data = {
        "table": table,
    }
    process(data)


@pytest.mark.parametrize("table", TABLES)
def test_manual(table):
    data = {
        "table": table,
        "start": START,
        "end": END,
    }
    process(data)
