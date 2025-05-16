"""Input related ui helpers"""

import time

from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait


class Input(object):
    """
    UI input related helpers
    """

    def __init__(self, driver):
        """
        Constructor for input class
        Args:
            driver(obj): web driver object
        """
        self.driver = driver

    def textbox(self, value, locator, clear=True):
        """
        This routine is used to place content in an input box.
        Args:
            value(any) : The value that you want to place in input box.
            locator(list): locator of the input box.
            clear(boolean): Set to True if you want to clear the input box before placing param <value> in it.
        """
        try:
            WebDriverWait(self.driver, 30).until(
                ec.presence_of_element_located(locator)
            )
            element = self.driver.find_element(*locator)
            if clear:
                element.clear()
            element.send_keys(value)
            time.sleep(1)
        except TimeoutException:
            raise NoSuchElementException("UI Element %s not found" % locator[1])
        except Exception as exp:
            raise exp

    def fileinput(self, file_path, locator):
        """
        This routine is used to perform fiel upload action from local
        Args :
             file_path(str) : location of file to be uploaded
             locator(list) : locator of upload button
        """
        try:
            WebDriverWait(self.driver, 30).until(
                ec.presence_of_element_located(locator)
            )
            element = self.driver.find_element(*locator)
            element.send_keys(file_path)
            time.sleep(1)
        except TimeoutException:
            raise NoSuchElementException("UI Element %s not found" % locator[1])
        except Exception as exp:
            raise exp
