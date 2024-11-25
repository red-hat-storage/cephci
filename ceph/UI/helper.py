from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


def capture_screen(driver, filename: str) -> str:
    """
    Captures a screenshot and saves it to the specified filename.
    :param driver: WebDriver instance.
    :param filename: The filename to save the screenshot as.
    :return: The path of the saved screenshot.
    """
    try:
        if driver.save_screenshot(filename):
            return filename
        else:
            raise RuntimeError(f"Screenshot capture failed for file: {filename}")
    except Exception as e:
        raise RuntimeError(f"Failed to capture screenshot: {e}")


def wait_for_element(driver, locator: str, locator_type: str = "id", timeout: int = 10):
    """
    Waits for a specific web element to become visible.
    :param driver: WebDriver instance.
    :param locator: The locator for the element.
    :param locator_type: The type of locator ('id', 'name', 'class', 'xpath', 'css').
    :param timeout: Maximum time to wait for the element to become visible (default: 10 seconds).
    :return: The WebElement if found within the timeout period.
    """
    by_types = {
        "id": By.ID,
        "name": By.NAME,
        "class": By.CLASS_NAME,
        "xpath": By.XPATH,
        "css": By.CSS_SELECTOR,
    }

    if locator_type not in by_types:
        raise ValueError(f"Unsupported locator type: {locator_type}")

    try:
        element = WebDriverWait(driver, timeout).until(
            EC.visibility_of_element_located((by_types[locator_type], locator))
        )
        return element
    except Exception as e:
        raise RuntimeError(
            f"Element with locator '{locator}' and type '{locator_type}' not visible within {timeout} seconds: {e}"
        )
