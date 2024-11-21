from browser import Browser
from operations import Operations
from helper import take_ss, wait_for_element

class Dashboard:
    def __init__(self, driver):
        """
        Initializes the Dashboard class with the provided WebDriver instance.
        Args:
            driver: The WebDriver instance to interact with the browser.
        """
        self.driver = driver

    def get_dashboard_data(self, element_id):
        """
        Retrieves and returns the text content of the dashboard element.
        Args:
            element_id: The ID of the dashboard element to retrieve text from.
        """
        try:
            dashboard_element = self.driver.find_element_by_id(element_id)
            return dashboard_element.text
        except Exception as e:
            print(f"Error getting dashboard data: {e}")
            return None

    def is_element_visible(self, element_id):
        """
        Checks whether an element is visible on the page.
        Args:
            element_id: The ID of the element whose visibility needs to be checked.
        """
        try:
            element = self.driver.find_element_by_id(element_id)
            return element.is_displayed()
        except Exception as e:
            print(f"Error checking visibility of element '{element_id}': {e}")
            return False

    def click_button(self, button_id):
        """
        Clicks a button identified by button_id.
        Args:
            button_id: The ID of the button to click.
        """
        try:
            button = self.driver.find_element_by_id(button_id)
            button.click()
        except Exception as e:
            print(f"Error clicking button '{button_id}': {e}")

    def perform_operations(self, username, password, username_field_id, password_field_id, login_button_id):
        """
        Perform login operation and other interactions after login.
        Args:
            username: The username to log in with.
            password: The password to log in with.
            username_field_id: The ID of the username field.
            password_field_id: The ID of the password field.
            login_button_id: The ID of the login button.
        """
        operations = Operations(self.driver)
        operations.login(username, password, username_field_id, password_field_id, login_button_id)
        take_ss(self.driver, "after_login_screenshot.png")  # Take a screenshot after login

# Main function to handle everything
def main():
    # Initialize Browser (You can change the type to "firefox" or other browsers if needed)
    browser = Browser(browser_type="chrome", driver_path="/path/to/chromedriver")

    # Open the URL
    url = input("Enter the URL to open: ")
    browser.open(url)

    # Initialize Dashboard
    dashboard = Dashboard(browser.driver)

    # Take a screenshot of the current page
    take_ss(browser.driver, "homepage_screenshot.png")

    # Wait for a specific element to be visible before proceeding
    element_id = input("Enter the ID of the element to check visibility: ")
    if wait_for_element(browser.driver, element_id):
        print(f"Element with ID '{element_id}' is visible.")
    else:
        print(f"Element with ID '{element_id}' is not visible.")

    # Perform login operation
    username = input("Enter username: ")
    password = input("Enter password: ")
    dashboard.perform_operations(username, password, "username_field", "password_field", "login_button")

    # Get dashboard data
    dashboard_data = dashboard.get_dashboard_data("dashboard_id")
    print(f"Dashboard Data: {dashboard_data}")

    # Click a button if visible
    if dashboard.is_element_visible("refresh_button"):
        dashboard.click_button("refresh_button")
    else:
        print("Refresh button not visible.")

    # Close the browser
    browser.close()

if __name__ == "__main__":
    main()
