"""
test_configuration.py

Tests to read configuration data from an optional configuration file.
"""

import unittest

from autocron import configuration

AUTOCRON_TEST_PROJECT_NAME = "autocron_test_storage"


class TestConfiguration(unittest.TestCase):

    def setUp(self):
        # create a fresh Configuration instance fo every test
        self.configuration = configuration.Configuration(
            project_name=AUTOCRON_TEST_PROJECT_NAME
        )
        self.configuration_file = self.configuration.configuration_file
        self.cd = self.configuration.__dict__.copy()

    def tearDown(self):
        if self.configuration_file.exists():
            self.configuration_file.unlink()

    def test_no_configuration_file(self):
        """
        The missing configuration file should raise no error and the
        default configuration should not change.
        """
        self.configuration._read_configuration()
        assert self.configuration.__dict__ == self.cd

    def test_malformed_configuration_file(self):
        """
        Configuration file should be in ini-format with an 'autocron'
        section. If this section is missing, the configuration file
        should get ignored.
        default configuration should not change.
        """
        content = """\
            [anothercron]
            result_ttl = 1800
        """.replace(" ", "")
        with open(self.configuration_file, "w") as fobj:
            fobj.write(content)
        self.configuration._read_configuration()
        assert self.configuration.__dict__ == self.cd

    def test_configuration_comments(self):
        """
        Create a configuration-file with a commented single setting.
        default configuration should not change.
        """
        content = """\
            [autocron]
            # result_ttl = 1800
        """.replace(" ", "")
        with open(self.configuration_file, "w") as fobj:
            fobj.write(content)
        self.configuration._read_configuration()
        assert self.configuration.__dict__ == self.cd

    def test_configuration_with_valid_setting(self):
        """
        Create a configuration-file with a single setting.
        default configuration should change.
        """
        value = 3600  # default would be 1800
        content = """\
            [autocron]
            result_ttl = 3600
        """.replace(" ", "")
        with open(self.configuration_file, "w") as fobj:
            fobj.write(content)
        self.configuration._read_configuration()
        assert self.configuration.__dict__ != self.cd
        assert self.configuration.result_ttl == value

    def test_configuration_is_active_is_true(self):
        """
        If is_active is given with a configparser true-value, it should
        be True.
        """
        assert self.configuration.is_active is True  # default
        content = """\
            [autocron]
            result_ttl = 3600
            is_active = yes
        """.replace(" ", "")
        with open(self.configuration_file, "w") as fobj:
            fobj.write(content)
        self.configuration._read_configuration()
        assert self.configuration.is_active is True

    def test_configuration_is_active_is_false(self):
        """
        If is_active is given with a configparser false-value, it should
        be False.
        """
        assert self.configuration.is_active is True  # default
        content = """\
            [autocron]
            result_ttl = 3600
            is_active = no
        """.replace(" ", "")
        with open(self.configuration_file, "w") as fobj:
            fobj.write(content)
        self.configuration._read_configuration()
        assert self.configuration.is_active is False

    def test_configuration_is_active_is_invalid(self):
        """
        If is_active is given with an invalid value, it should
        be None.
        """
        assert self.configuration.is_active is True  # default
        content = """\
            [autocron]
            result_ttl = 3600
            is_active = invalid
        """.replace(" ", "")
        with open(self.configuration_file, "w") as fobj:
            fobj.write(content)
        self.configuration._read_configuration()
        assert self.configuration.is_active is True
