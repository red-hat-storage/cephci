from browser import Browser
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


class Dashboard:
    def __init__(self, browser_type: str = "chrome"):
        """
        Initializes the Dashboard with a specified browser type.
        :param browser_type: The type of browser to use ('chrome', 'firefox', etc.).
        """
        self.browser = Browser(browser_type)
        self.driver = (
            self.browser.driver
        )  # Access the WebDriver instance directly from Browser class

    def open(self, url: str):
        """
        Navigates to the specified URL.
        :param url: The URL to open in the browser.
        """
        if not isinstance(url, str):
            raise ValueError("A valid URL must be provided.")
        self.driver.get(url)

    def is_element_visible(
        self, locator: str, timeout: int = 10, element_identifier_type: str = "id"
    ) -> bool:
        """
        Checks if an element is visible on the page within the given timeout.
        :param locator: The value of the locator (ID, Name, XPath, etc.).
        :param timeout: Maximum time to wait for the element to be visible (default: 10 seconds).
        :param element_identifier_type: The type of element identifier (id, name, class, xpath, css).
        :return: True if the element is visible, False otherwise.
        """
        if not isinstance(locator, str):
            raise ValueError("A valid locator must be provided.")

        by_locator = self._get_by_locator(element_identifier_type)

        try:
            element = WebDriverWait(self.driver, timeout).until(
                EC.visibility_of_element_located((by_locator, locator))
            )
            return element.is_displayed()
        except TimeoutException:
            return False

    def click(
        self, locator: str, timeout: int = 10, element_identifier_type: str = "id"
    ):
        """
        Clicks a button or element identified by its locator.
        :param locator: The value of the locator (ID, Name, XPath, etc.).
        :param timeout: Maximum time to wait for the element to be clickable (default: 10 seconds).
        :param element_identifier_type: The type of element identifier (id, name, class, xpath, css).
        """
        if not isinstance(locator, str):
            raise ValueError("A valid locator must be provided.")

        # Map the element_identifier_type to the appropriate By method
        by_locator = self._get_by_locator(element_identifier_type)

        button = WebDriverWait(self.driver, timeout).until(
            EC.element_to_be_clickable((by_locator, locator))
        )
        button.click()

    def _get_by_locator(self, locator_type: str):
        """
        Maps locator type to Selenium By class.
        :param locator_type: The type of locator (id, name, class, xpath, css, etc.)
        :return: Corresponding By locator method.
        """
        locator_type = locator_type.lower()

        if locator_type == "id":
            return By.ID
        elif locator_type == "name":
            return By.NAME
        elif locator_type == "class":
            return By.CLASS_NAME
        elif locator_type == "xpath":
            return By.XPATH
        elif locator_type == "css":
            return By.CSS_SELECTOR
        else:
            raise ValueError(
                f"Unsupported locator type: {locator_type}. Supported types are 'id', 'name', 'class', 'xpath', 'css'."
            )
