import os

import pytest
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService


@pytest.fixture(scope="function")
def driver(request):
    """
    Conftest to Initialize Selenium Webdriver tests.
    1. Opens the browser.
    2. Opens URL.
    3. Maximizes browser window.
    Args:
    request (object): gives access to the requesting test context.
    Raises: NA
    Returns: selenium webdriver instance
    """
    hub_host = ""
    if os.getenv("browser") is None or os.getenv("browser") == "chrome":
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("disable-infobars")
        chrome_options.add_argument("enable-automation")
        wdriver = webdriver.Chrome(service=ChromeService(), options=chrome_options)
    else:
        fp = webdriver.FirefoxProfile()
        fp.set_preference("dom.max_chrome_script_run_time", 60)
        fp.set_preference("dom.max_script_run_time", 60)
        firefox_options = webdriver.FirefoxOptions()
        firefox_options.add_argument("--headless")
        firefox_options.add_argument("--no-sandbox")
        firefox_options.add_argument("--disable-extensions")
        firefox_options.add_argument("disable-infobars")
        firefox_options.add_argument("enable-automation")
        firefox_options.accept_insecure_certs = True

        fp = webdriver.FirefoxProfile()
        fp.accept_untrusted_certs = True
        firefox_options.profile = fp

        wdriver = webdriver.Remote(
            command_executor="http://{}:4444".format(hub_host),
            desired_capabilities={
                "browserName": "firefox",
                "javascriptEnabled": True,
                "acceptSslCerts": True,
                "acceptInsecureCerts": True,
            },
            options=firefox_options,
        )

    wdriver.maximize_window()
    wdriver.set_page_load_timeout(50)
    wdriver.implicitly_wait(10)
    wdriver.set_script_timeout(10)
    product_url = ""  # dashboard url
    wdriver.get("http://{product_url}".format(product_url=product_url))

    def end():
        wdriver.quit()
        # display.stop()

    request.addfinalizer(end)
    return wdriver
