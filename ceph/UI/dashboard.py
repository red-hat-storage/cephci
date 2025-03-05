from browser import Browser
from ids import ElementIDs


class Dashboard(Browser):
    def __init__(self, yaml_file_path):
        super().__init__()
        self.element_ids = ElementIDs(yaml_file_path)

    def login(self, username=None, email=None, password=None):
        """Logs into the Ceph dashboard using either username or email."""
        if email:
            login_type = "email_login"
            self.enter_text(
                self.element_ids.get_element(login_type, "email")["xpath"], email
            )
        elif username:
            login_type = "username_login"
            self.enter_text(
                self.element_ids.get_element(login_type, "username")["xpath"], username
            )
        else:
            raise ValueError("Either username or email must be provided!")

        self.enter_text(
            self.element_ids.get_element(login_type, "password")["xpath"], password
        )
        self.click_element(
            self.element_ids.get_element(login_type, "login_button")["xpath"]
        )
        print("Login successful!")
