from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


class ElementIDs:
    def __init__(self, dashboard):
        """
        Initializes the ElementIDs with the Dashboard class instance.
        :param dashboard: An instance of the Dashboard class.
        """
        self.dashboard = dashboard

    def get_element(self, page: str, element: str):
        """
        Fetches the locator for the specified page and element by calling appropriate Dashboard methods.
        :param page: The page name (e.g., 'username_login', 'email_login', 'common').
        :param element: The element name (e.g., 'login_username', 'sign_in_button').
        :return: Locator dictionary for the specified element.
        """
        try:
            # Dynamically call the corresponding Dashboard method based on the page name
            if page == "username_login":
                locators = self.dashboard.get_username_login()
            elif page == "email_login":
                locators = self.dashboard.get_email_login()
            elif page == "common":
                locators = self.dashboard._get_common()
            else:
                raise ValueError(f"Unknown page: {page}")

            # Convert tuples to a dictionary (for methods like _get_common)
            if isinstance(locators, tuple):
                locators = {k: v for d in locators for k, v in d.items()}

            # Retrieve the specific element from the locators
            if element in locators:
                return locators[element]
            else:
                raise KeyError(
                    f"Element '{element}' not found in locators for page '{page}'."
                )

        except Exception as e:
            raise RuntimeError(
                f"Failed to get element '{element}' for page '{page}': {e}"
            )

    def _get_by_type(self, locator_type: str):
        """
        Maps a locator type to the Selenium By class.
        :param locator_type: The type of the locator (e.g., 'id', 'xpath').
        :return: The Selenium By type.
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
            raise ValueError(f"Unsupported locator type: {locator_type}")
