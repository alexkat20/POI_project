from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.common.exceptions import NoSuchElementException
from bs4 import BeautifulSoup
from time import sleep
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support import expected_conditions as EC


class GrabberApp:

    def __init__(self, city):
        self.city = city

    def grab_data(self):
        # chrome_options = webdriver.ChromeOptions
        # chrome_options.add_argument("--no-sandbox")
        # chrome_options.add_argument("--window-size=1420,1080")
        # chrome_options.add_argument("--headless")
        # chrome_options.add_argument("--disable-gpu")

        options = webdriver.ChromeOptions()
        options.headless = False
        options.page_load_strategy = "none"

        chrome_path = ChromeDriverManager().install()
        chrome_service = Service(chrome_path)

        driver = Chrome(options=options, service=chrome_service)

        driver.maximize_window()
        driver.get('https://yandex.ru/maps')
        sleep(5)

        # Вводим данные поиска
        driver.find_element(
            By.XPATH, f'(//input[contains(@class,"input__control _bold")])'
        ).send_keys(self.city)

        # Нажимаем на кнопку поиска
        # driver.find_element(By.CLASS_NAME, 'small-search-form-view__button').click()
        driver.find_element(
            By.XPATH, f'(//div[contains(@class,"small-search-form-view__icon _type_search")])'
        ).click()
        sleep(5)
        driver.find_element(
            By.XPATH, f'(//div[contains(@class,"tabs-select-view__title _name_inside")])'
        ).click()
        sleep(5)

        parent_handle = driver.window_handles[0]

        for i in range(20):
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located(
                    (By.XPATH, f'(//a[contains(@class,"search-business-snippet-view__rating")])')
                )
            )
            review = driver.find_element(
                By.XPATH, f'(//a[contains(@class,"search-business-snippet-view__rating")])[{i+1}]'
            )

            print(review.get_attribute("href"))

            url = review.get_attribute("href")

            review.location_once_scrolled_into_view

            driver.execute_script(f'window.open("{url}","org_tab");')

            child_handle = [x for x in driver.window_handles if x != parent_handle][0]
            driver.switch_to.window(child_handle)

        driver.quit()


def main():
    city = input('Область поиска: ')
    grabber = GrabberApp(city)
    grabber.grab_data()


if __name__ == '__main__':
    main()