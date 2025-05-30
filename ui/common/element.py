""" This is a file containing all element releated operations"""
import selenium.webdriver.support.ui as ui
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support import expected_conditions as EC


class Element(object):
    """
    This module contains WebElement Related helper methods
    """

    def __init__(self, driver):
        """
        Constructor for Element class
        Args:
            driver(object): Web driver object
        """
        self.driver = driver

    def is_element_present(self, locator):
        """
        This routines returns True if locator element param <locator> is present on UI else False.
        Args:
            locator(tuple): Web element locator
        Returns:
             boolean: True  if param <locator> element is present on UI else False.
        """
        try:
            self.driver.find_element(*locator)
        except Exception:
            return False
        return True

    def is_element_absent(self, locator):
        """
        This routines returns True if locator element param <locator> is absent on UI else False.
        Args:
            locator(tuple): Web element locator
        Returns:
            boolean: True  if param <locator> element is absent on UI else False.
        """
        try:
            self.driver.find_element(*locator)
        except Exception:
            return True
        return False

    def is_visible(self, element, timeout=2):
        """
        Return True if element is visible within 2 seconds, otherwise False
        Args:
            element (tuple): Element locator
            timeout (int) : Timeout secs for webdriver wait
        Returns:
            True if element is visible within 2 seconds, otherwise False
        """
        try:
            ui.WebDriverWait(self.driver, timeout).until(
                EC.visibility_of_element_located(element)
            )
            return True
        except TimeoutException:
            return False

    def is_not_visible(self, element, timeout=2):
        """
        Return True if element is not visible within 2 seconds, otherwise False
        Args:
            element (tuple): Element locator
            timeout (int) : Timeout secs for webdriver wait
        Returns:
            True if element is not visible within 2 seconds, otherwise False
        """
        try:
            ui.WebDriverWait(self.driver, timeout).until_not(
                EC.visibility_of_element_located(element)
            )
            return True
        except TimeoutException:
            return False

    def get_current_url(self):
        """
        This routine returns current url of the browser
        Returns:
            str : Current url of the browser
        """
        return self.driver.current_url

    def get_text(self, locator):
        """
        Gets text of the element
        Args:
            locator: locator of the element
        Returns:
            Text of the element
        """
        _element = self.driver.find_element(*locator)
        return _element.text
