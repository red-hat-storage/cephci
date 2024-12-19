from selenium import webdriver


class Browser:
    def __init__(self, browser_type: str = "chrome"):
        """
        Initializes the Browser with the specified browser type.
        :param browser_type: The type of browser to use ('chrome', 'firefox', etc.). Defaults to 'chrome'.
        """
        if browser_type.lower() == "chrome":
            self.driver = webdriver.Chrome()
        elif browser_type.lower() == "firefox":
            self.driver = webdriver.Firefox()
        else:
            raise ValueError(
                f"Unsupported browser type: {browser_type}. Supported types are 'chrome' and 'firefox'."
            )
