import os

import yaml
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


class ElementIDs:
    def __init__(self, yaml_file_path):
        """
        Initializes the ElementIDs by loading locators from a YAML file.
        :param yaml_file_path: The path to the YAML file containing the locators.
        """
        self.yaml_file_path = yaml_file_path
        self.locators = self._load_locators()

    def _load_locators(self):
        """Loads locators from the specified YAML file."""
        try:
            with open(self.yaml_file_path, "r") as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            raise FileNotFoundError(f"YAML file not found: {self.yaml_file_path}")
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing YAML file: {e}")

    def get_element(self, page: str, element: str):
        """
        Fetches the locator for the specified page and element from the loaded YAML file.
        :param page: The page name (e.g., 'username_login', 'email_login', 'common').
        :param element: The element name (e.g., 'username', 'sign_in_button').
        :return: Locator dictionary for the specified element.
        """
        try:
            if page in self.locators and element in self.locators[page]:
                return self.locators[page][element]
            else:
                raise KeyError(
                    f"Element '{element}' not found in page '{page}' in the YAML file."
                )
        except KeyError as e:
            raise KeyError(f"Error retrieving element: {e}")
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
