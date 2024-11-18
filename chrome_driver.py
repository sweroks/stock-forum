from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import asyncio


class WebDriverPool:
    def __init__(self, num_drivers=5):
        self.drivers = []
        for _ in range(num_drivers):
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--verbose")
            driver = webdriver.Chrome(options=chrome_options)
            self.drivers.append(driver)
        self.semaphore = asyncio.Semaphore(num_drivers)

    async def get_driver(self):
        await self.semaphore.acquire()
        return self.drivers.pop()

    def release_driver(self, driver):
        self.drivers.append(driver)
        self.semaphore.release()

    def close_all(self):
        for driver in self.drivers:
            driver.quit()