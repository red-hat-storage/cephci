""" common UI helpers"""


def scroll_into_view(driver, element):
    """
    This routine scrolls the view to the <element>.
    Args:
        driver(object): web driver object
        element(tuple): web element locator to which you want to scroll
    """
    element = driver.find_element(*element)
    driver.execute_script("return arguments[0].scrollIntoView();", element)
