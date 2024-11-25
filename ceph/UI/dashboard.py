from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from ceph.UI.browser import Browser
from ceph.UI.ids import ElementIDs


class Dashboard(Browser):
    def __init__(self, browser_type: str = "chrome"):
        super().__init__(browser_type=browser_type)
        self.element_ids = None  # ElementIDs instance will be set dynamically

    def login(self, username: str, password: str):
        username_key, password_key, login_button_key = self.get_username_login()
        username_field = self.find_element(username_key)
        username_field.clear()
        username_field.send_keys(username)

        password_field = self.find_element(password_key)
        password_field.clear()
        password_field.send_keys(password)

        self.click(login_button_key)
        print("Login successful!")

    def _get_common(self):
        sign_in_button = {
            "xpath": "//button[contains(text(), 'Sign In')]",
            "id": "sign-in",
        }

        sign_up_button = {
            "xpath": "//button[contains(text(), 'Sign Up')]",
            "id": "sign-up",
        }
        return sign_in_button, sign_up_button

    def get_username_login(self):
        login_username = {
            "id": "username",
            "xpath": "//input[contains(@placeholder, 'username')]",
        }

        login_password = {
            "id": "password",
            "xpath": "//input[contains(@placeholder, 'password')]",
        }

        login_button = {"id": "submit", "xpath": "//button[contains(text(), 'Login')]"}
        return login_username, login_password, login_button

    def get_email_login(self):

        login_username = {"name": "email", "xpath": "//input[@type='email']"}
        login_password = {"name": "pass", "xpath": "//input[@type='password']"}
        login_button = {"xpath": "//button[@type='submit']"}
        sign_up_email = {"xpath": "//input[@type='email']"}
        sign_up_password = {"xpath": "//input[@type='password']"}
        sign_up_button = {"xpath": "//button[contains(text(), 'Register')]"}
        return (
            login_username,
            login_password,
            login_button,
            sign_up_email,
            sign_up_password,
            sign_up_button,
        )
