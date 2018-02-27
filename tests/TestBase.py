import os
import unittest

import ProjectUtils
from core.SystemSim import config_schema
from utils import SimUtils

class BaseTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(BaseTest, self).__init__(*args, **kwargs)

        config_file_path = os.path.join(ProjectUtils.root_path, 'tests', 'test_config.ini')
        self.config = SimUtils.load_config(config_file_path, config_schema)

    def check_file(self, path, should_exist=False):
        """ Tests if a file exists or not given the shouldExist flag."""
        self.assertEqual(os.path.isfile(path), should_exist, msg='File: %s' % (path))
