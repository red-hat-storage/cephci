import os

from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from ceph.UI.ids import ElementIDs


class Browser:
    def __init__(self, browser_type: str = "chrome", headless: bool = False):
        """
        Initializes the Browser with basic configurations.
        :param browser_type: The type of browser to use ('chrome', 'firefox'). Defaults to 'chrome'.
        :param headless: Whether to run the browser in headless mode. Defaults to False.
        """
        self.browser_type = browser_type.lower()
        self.headless = headless
        self.driver = self._initialize_browser()
        self.element_ids = None  # ADDED

    def _initialize_browser(self):
        """
        Dynamically initializes the WebDriver based on the browser type.
        """
        if self.browser_type == "chrome":
            return self._setup_chrome()
        elif self.browser_type == "firefox":
            return self._setup_firefox()
        else:
            raise ValueError(
                f"Unsupported browser type: {self.browser_type}. Supported browsers are 'chrome' and 'firefox'."
            )

    def _apply_headless(self, options):
        """
        Applies headless mode if enabled.
        :param options: Options object for the browser (ChromeOptions or FirefoxOptions).
        """
        if self.headless:
            if isinstance(options, ChromeOptions):
                options.add_argument("--headless=new")
            else:
                options.add_argument("--headless")

    def _setup_chrome(self):
        """
        Sets up the Chrome WebDriver with basic configurations.
        """
        options = ChromeOptions()
        self._apply_headless(options)
        return webdriver.Chrome(options=options)

    def _setup_firefox(self):
        """
        Sets up the Firefox WebDriver with basic configurations.
        """
        options = FirefoxOptions()
        self._apply_headless(options)
        return webdriver.Firefox(options=options)

    def open(self, url: str):
        if not isinstance(url, str):
            raise ValueError("A valid URL must be provided.")
        self.driver.get(url)

    def quit(self):
        self.driver.quit()

    def load_elements(self, yaml_file_path: str):
        self.element_ids = ElementIDs(yaml_file_path)

    def is_element_visible(self, key: str, timeout: int = 10) -> bool:
        element = self.find_element(key, timeout)
        return element.is_displayed() if element else False

    def find_element(self, key: str, timeout: int = 10):
        if not self.element_ids:
            raise RuntimeError("ElementIDs not loaded. Use 'load_elements' first.")

        try:
            page, element = key.split(".")
        except ValueError:
            raise ValueError(
                "Invalid key format.  Key must be in 'page.element' format."
            )

        locator = self.element_ids.get_element(page, element)
        by_type_str = list(locator.keys())[
            0
        ]  # Get locator type string ("id", "xpath", etc.)
        locator_value = locator[by_type_str]  # Get the locator value
        by_type = self.element_ids._get_by_type(by_type_str)  # Get the By Type

        try:
            element = WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((by_type, locator_value))
            )
            return element
        except Exception as e:
            raise RuntimeError(f"Element with key '{key}' not found: {e}")

    def click(self, key: str, timeout: int = 10):
        element = self.find_element(key, timeout)
        WebDriverWait(self.driver, timeout).until(EC.element_to_be_clickable(element))
        element.click()
