from dashboard import Dashboard


class Operations:
    def __init__(self, driver):
        """Initialize Operations with a WebDriver instance."""
        self.driver = driver
        self.dashboard = Dashboard(driver)

    def login(self, username: str, password: str):
        """Performs a login operation using the provided credentials."""
        try:
            username_field = self.driver.find_element_by_id("username")
            password_field = self.driver.find_element_by_id("password")
            login_button = self.driver.find_element_by_id("login")

            username_field.clear()
            username_field.send_keys(username)
            password_field.clear()
            password_field.send_keys(password)

            login_button.click()
        except Exception as e:
            raise RuntimeError(f"Login failed: {e}")
