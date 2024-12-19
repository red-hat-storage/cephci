from dashboard import Dashboard


class Operations:
    def __init__(self, browser_type: str):
        """
        Initialize Operations with a browser type.
        :param browser_type: The type of browser to initialize ('chrome', 'firefox', etc.).
        """
        self.dashboard = Dashboard(browser_type)

    def login(
        self,
        username: str,
        password: str,
        username_locator: str,
        password_locator: str,
        login_button_locator: str,
        username_locator_type: str = "id",
        password_locator_type: str = "id",
        login_button_locator_type: str = "id",
    ):
        """
        Performs a login operation using the provided credentials.
        :param username: The username for login.
        :param password: The password for login.
        :param username_locator: The locator for the username field.
        :param password_locator: The locator for the password field.
        :param login_button_locator: The locator for the login button.
        :param username_locator_type: The locator type for the username field ('id', 'name', 'class', 'xpath', 'css').
        :param password_locator_type: The locator type for the password field ('id', 'name', 'class', 'xpath', 'css').
        :param login_button_locator_type: The locator type for the login button ('id', 'name', 'class', 'xpath', 'css').
        """
        try:

            username_field = self.dashboard.find_element(
                username_locator, username_locator_type
            )
            username_field.clear()
            username_field.send_keys(username)

            password_field = self.dashboard.find_element(
                password_locator, password_locator_type
            )
            password_field.clear()
            password_field.send_keys(password)

            self.dashboard.click(
                login_button_locator, locator_type=login_button_locator_type
            )
        except Exception as e:
            raise RuntimeError(f"Login failed: {e}")
