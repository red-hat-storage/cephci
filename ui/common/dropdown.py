"""Helper for Drop-down WebElement"""

from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait

from ui.common.click import Click
from ui.common.common import scroll_into_view
from ui.common.element import Element
from ui.common.input import Input


class Dropdown(object):
    """
    Contains web element Drop-down related methods.
    """

    def __init__(self, driver):
        """
        Constructor for the Dropdown class
        Args:
            driver(object): web driver object
        """
        self.driver = driver
        self.click = Click(driver)
        self.input = Input(driver)
        self.browser = Element(driver)

    def select_by_label_xpath(self, label_xpath, value_locator):
        """
        This routines enable to select an option from dropdown using label_xpath
        Args:
            label_xpath(list) : label xpath by xpath locator
            value_to_select(str) : dropdown value
        """
        try:
            WebDriverWait(self.driver, 30).until(
                ec.visibility_of_element_located(label_xpath)
            )
            scroll_into_view(self.driver, label_xpath)
            self.click.button(label_xpath)
            scroll_into_view(self.driver, value_locator)
            self.click.button(value_locator)
        except TimeoutException:
            raise NoSuchElementException("UI Element %s not found" % label_xpath[1])
        except Exception as exp:
            raise exp
