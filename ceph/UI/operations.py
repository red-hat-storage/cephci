from ceph.UI.dashboard import Dashboard


class Operations:
    def __init__(self, browser_type: str):
        self.dashboard = Dashboard(browser_type)

    def perform_click(self, element_key: str):
        self.dashboard.click(element_key)

    def perform_input(self, element_key: str, text: str):
        element = self.dashboard.find_element(element_key)
        element.clear()
        element.send_keys(text)
