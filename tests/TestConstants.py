from core import Constants
from tests.TestBase import BaseTest


class TestConstants(BaseTest):
    def test_unique_values(self):
        unique_set = set()

        # Loop over all attributes in this module, skip callables and built-in attributes.
        for value in [getattr(Constants, attr) for attr in dir(Constants) if
                      not callable(getattr(Constants, attr)) and not attr.startswith("__")]:
            self.assertFalse(value in unique_set)
            unique_set.add(value)
