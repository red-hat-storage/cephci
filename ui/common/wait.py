"""wait helpers"""

import logging

import selenium.webdriver.support.expected_conditions as EC
import selenium.webdriver.support.ui as ui
from common.utils.logger import configure_log
from selenium.common.exceptions import NoSuchElementException, TimeoutException

LOG = configure_log(logging.DEBUG, __name__, "wait_facade.log")


class WaitHelper(object):
    """Helper for different waits"""

    def __init__(self, wdriver, timeout=300):
        """
        Initializes the Web driver wait
        Args:
            wdriver (object): Selenium webdriver object.
            timeout (int) : Timeout value for a wait
        """
        self.wait = ui.WebDriverWait(wdriver, timeout)
        self.driver = wdriver

    def wait_until_element_present(self, element):
        """
        Waits until element is present
        Args:
            element (tuple): Element locator
        """
        try:
            self.wait.until(EC.presence_of_element_located(element))
        except TimeoutException:
            raise NoSuchElementException("UI Element %s not found" % element[1])
        except Exception as exce:
            raise exce

    def wait_until_element_visible(self, element):
        """
        Waits until element is visible in timeout secs, if not throws exception
        Args:
            element (tuple): Element locator
        """
        LOG.info("Waiting for '%s' element to get visible" % element[1])
        try:
            self.wait.until(EC.visibility_of_element_located(element))
        except TimeoutException:
            raise NoSuchElementException("UI Element %s not found" % element[1])
        except Exception as exce:
            raise exce

    def wait_until_element_is_clickable(self, element):
        """
        Waits until element becomes clickable until timeout secs
        Args:
            element (tuple): Element locator
        """
        try:
            self.wait.until(EC.element_to_be_clickable(element))
        except TimeoutException:
            raise NoSuchElementException("UI Element %s not found" % element[1])
        except Exception as exce:
            raise exce
