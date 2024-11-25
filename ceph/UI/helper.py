from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


def take_ss(driver, filename: str):
    """Takes a screenshot and saves it to the specified filename."""
    try:
        driver.save_screenshot(filename)
    except Exception as e:
        raise RuntimeError(f"Failed to take screenshot: {e}")


def wait_for_element(driver, element_id: str, timeout: int = 10):
    """Waits for a specific web element to become visible."""
    try:
        WebDriverWait(driver, timeout).until(
            EC.visibility_of_element_located((By.ID, element_id))
        )
    except Exception as e:
        raise RuntimeError(
            f"Element with ID '{element_id}' not visible within {timeout} seconds: {e}"
        )
