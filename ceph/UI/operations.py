from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from helper import wait_for_element


class Operations:
    def __init__(self, driver: WebDriver):
        """
        Initializes Operations with a WebDriver instance.
        Args:
            driver: A WebDriver instance to perform operations on.
        """
        self.driver = driver

    def login(self, username: str, password: str, username_field_id: str, password_field_id: str, login_button_id: str):
        """
        Performs a login action on the page.
        Args:
            username: The username to use for login.
            password: The password to use for login.
            username_field_id: The ID of the username input field.
            password_field_id: The ID of the password input field.
            login_button_id: The ID of the login button.
        """
        try:
            print(f"Attempting to log in with username: {username}")

            # Wait for the username field to be visible and then input username
            wait_for_element(self.driver, username_field_id, timeout=10)
            username_field = self.driver.find_element(By.ID, username_field_id)
            username_field.clear()  # Clear any pre-filled text
            username_field.send_keys(username)

            # Wait for the password field to be visible and then input password
            wait_for_element(self.driver, password_field_id, timeout=10)
            password_field = self.driver.find_element(By.ID, password_field_id)
            password_field.clear()  # Clear any pre-filled text
            password_field.send_keys(password)

            # Wait for the login button to be clickable and then click it
            wait_for_element(self.driver, login_button_id, timeout=10)
            login_button = self.driver.find_element(By.ID, login_button_id)
            login_button.click()
            print("Login button clicked")

        except Exception as e:
            print(f"Error during login: {e}")

    def logout(self, logout_button_id: str):
        """
        Performs logout action.
        Args:
            logout_button_id: The ID of the logout button.
        """
        try:
            # Wait for the logout button to be visible and then click it
            wait_for_element(self.driver, logout_button_id, timeout=10)
            logout_button = self.driver.find_element(By.ID, logout_button_id)
            logout_button.click()
            print("Logout button clicked")

        except Exception as e:
            print(f"Error during logout: {e}")

    def search(self, search_field_id: str, query: str, search_button_id: str):
        """
        Performs a search operation by typing a query and clicking the search button.
        Args:
            search_field_id: The ID of the search input field.
            query: The search query to enter.
            search_button_id: The ID of the search button.
        """
        try:
            # Wait for the search field to be visible and enter the search query
            wait_for_element(self.driver, search_field_id, timeout=10)
            search_field = self.driver.find_element(By.ID, search_field_id)
            search_field.clear()  # Clear any pre-filled text
            search_field.send_keys(query)

            # Wait for the search button to be clickable and then click it
            wait_for_element(self.driver, search_button_id, timeout=10)
            search_button = self.driver.find_element(By.ID, search_button_id)
            search_button.click()
            print(f"Search for '{query}' initiated")

        except Exception as e:
            print(f"Error during search: {e}")

    def click_link(self, link_text: str):
        """
        Clicks a link on the page identified by its text.
        Args:
            link_text: The text of the link to click.
        """
        try:
            # Wait for the link to be visible and then click it
            wait_for_element(self.driver, link_text, timeout=10)
            link = self.driver.find_element(By.LINK_TEXT, link_text)
            link.click()
            print(f"Link '{link_text}' clicked")

        except Exception as e:
            print(f"Error clicking link '{link_text}': {e}")

    def fill_form(self, form_fields: dict, submit_button_id: str):
        """
        Fills a form with provided data and submits it.
        Args:
            form_fields: A dictionary with field IDs as keys and input data as values.
            submit_button_id: The ID of the submit button.
        """
        try:
            for field_id, value in form_fields.items():
                # Wait for the form field to be visible and then input the value
                wait_for_element(self.driver, field_id, timeout=10)
                form_field = self.driver.find_element(By.ID, field_id)
                form_field.clear()  # Clear any pre-filled text
                form_field.send_keys(value)
                print(f"Filled field '{field_id}' with value: {value}")

            # Wait for the submit button to be clickable and then click it
            wait_for_element(self.driver, submit_button_id, timeout=10)
            submit_button = self.driver.find_element(By.ID, submit_button_id)
            submit_button.click()
            print("Form submitted")

        except Exception as e:
            print(f"Error filling form: {e}")
