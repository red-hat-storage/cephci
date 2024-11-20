from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class Dashboard:
    def __init__(self, driver):
        """
        Initialize the Dashboard class with a WebDriver instance.

        Args:
            driver (WebDriver): An instance of Selenium WebDriver.
        """
        self.driver = driver

    def is_element_visible(self, element_id, timeout=10):
        """
        Check if an element is visible on the dashboard.

        Args:
            element_id (str): The ID of the element to check.
            timeout (int, optional): How long to wait for the element to become visible. Defaults to 10 seconds.


        """
        try:
            WebDriverWait(self.driver, timeout).until(
                EC.visibility_of_element_located((By.ID, element_id))
            )
            return True
        except Exception:
            return False

    def click(self, button_id):
        """
        Click a button identified by its ID.

        Args:
            button_id (str): The ID of the button to click.


        """
        try:
            button = self.driver.find_element(By.ID, button_id)
            button.click()
        except Exception as e:
            print(f"Error clicking button with ID '{button_id}': {e}")