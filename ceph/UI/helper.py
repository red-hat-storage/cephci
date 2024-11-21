from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os


def take_ss(driver, filename):
    """
    Takes a screenshot of the current page and saves it to a file.

    Args:
        driver: The WebDriver instance.
        filename: The name of the file where the screenshot will be saved.
    """
    try:
        # Define the directory where screenshots will be stored
        screenshot_directory = "screenshots"

        # Create the directory if it doesn't exist
        if not os.path.exists(screenshot_directory):
            os.makedirs(screenshot_directory)

        # Take the screenshot
        screenshot_path = os.path.join(screenshot_directory, filename)
        driver.save_screenshot(screenshot_path)
        print(f"Screenshot saved at: {screenshot_path}")
    except Exception as e:
        print(f"Error taking screenshot: {e}")


def wait_for_element(driver, element_id, timeout=10):
    """
    Waits for an element to be visible on the page.

    Args:
        driver: The WebDriver instance.
        element_id: The ID of the element to wait for.
        timeout: The maximum time to wait for the element (default is 10 seconds).


    """
    try:
        # Wait for the element to be visible using WebDriverWait
        element = WebDriverWait(driver, timeout).until(
            EC.visibility_of_element_located((By.ID, element_id))
        )
        print(f"Element with ID '{element_id}' is visible.")
        return element
    except Exception as e:
        print(f"Error waiting for element '{element_id}': {e}")
        return None
