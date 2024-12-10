from selenium.webdriver.common.by import By


def read_table(driver, table_ref: str, locator_type: str = "id") -> list:
    """
    Reads the data from a table on a webpage.
    :param driver: WebDriver instance.
    :param table_ref: The reference to locate the table (e.g., ID, class, XPath, etc.).
    :param locator_type: The type of locator ('id', 'name', 'class', 'xpath', 'css'). Defaults to 'id'.
    :return: A list of lists, where each sublist represents a row in the table.
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

        table_element = driver.find_element(by_types[locator_type], table_ref)

        rows = table_element.find_elements(By.TAG_NAME, "tr")

        table_data = []
        for row in rows:
            cells = row.find_elements(By.TAG_NAME, "td")
            if not cells:
                cells = row.find_elements(By.TAG_NAME, "th")
            row_data = [cell.text.strip() for cell in cells]
            table_data.append(row_data)

        return table_data
    except Exception as e:
        raise RuntimeError(f"Failed to read table with reference '{table_ref}': {e}")
