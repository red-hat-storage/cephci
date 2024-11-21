from selenium import webdriver
from selenium.webdriver.chrome.service import Service


class Browser:
    def __init__(self, browser_type="chrome", driver_path=None):
        """
        Initializes the Browser object with the specified browser type and WebDriver path.
        """
        self.driver = self._initialize_driver(browser_type, driver_path)

    def _initialize_driver(self, browser_type, driver_path):
        """
        Initializes the WebDriver based on the provided browser type.
        """
        if browser_type.lower() == "chrome":
            options = webdriver.ChromeOptions()
            service = Service(executable_path=driver_path)
            driver = webdriver.Chrome(service=service, options=options)
        elif browser_type.lower() == "firefox":
            options = webdriver.FirefoxOptions()
            service = Service(executable_path=driver_path)
            driver = webdriver.Firefox(service=service, options=options)
        else:
            raise ValueError(f"Unsupported browser type: {browser_type}")

        driver.implicitly_wait(10)  # Implicit wait for the driver
        return driver

    def open(self, url):
        """
        Opens the provided URL in the browser.
        """
        self.driver.get(url)

    def close(self):
        """
        Closes the browser window.
        """
        self.driver.quit()
