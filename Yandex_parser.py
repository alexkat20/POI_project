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

# Миасс, проспект Автозаводцев, 21

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
        sleep(2)

        # Вводим данные поиска
        driver.find_element(
            By.XPATH, f'(//input[contains(@class,"input__control _bold")])'
        ).send_keys(self.city)

        # Нажимаем на кнопку поиска
        # driver.find_element(By.CLASS_NAME, 'small-search-form-view__button').click()
        driver.find_element(
            By.XPATH, f'(//div[contains(@class,"small-search-form-view__icon _type_search")])'
        ).click()
        sleep(2)
        driver.find_element(
            By.XPATH, f'(//div[contains(@class,"tabs-select-view__title _name_inside")])'
        ).click()
        sleep(2)

        parent_handle = driver.window_handles[0]

        i = 1
        while i:
            sleep(1)

            try:
                page = driver.find_element(
                    By.XPATH, f'(//a[contains(@class,"search-business-snippet-view__rating")])[{i}]'
                )
                print(page.get_attribute("href"))
            except:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located(
                        (By.XPATH, f'(//div[contains(@class,"search-business-snippet-view__title")])[{i}]')
                    )
                )
                driver.find_element(
                    By.XPATH, f'(//div[contains(@class,"search-business-snippet-view__title")])[{i}]'
                ).location_once_scrolled_into_view
                i += 1
                continue




            url = page.get_attribute("href")

            page.location_once_scrolled_into_view

            driver.execute_script(f'window.open("{url}","org_tab");')

            child_handle = [x for x in driver.window_handles if x != parent_handle][0]
            driver.switch_to.window(child_handle)

            sleep(2)
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located(
                        (By.XPATH, f'(//div[contains(@class,"tabs-select-view__title _name_reviews _selected")])')
                    )
                )

                review_number = driver.find_element(
                    By.XPATH, f'(//div[contains(@class,"tabs-select-view__title _name_reviews _selected")])'
                ).get_attribute("aria-label").split(", ")[1]

                print(review_number)

                j = 0
            except:
                driver.close()
                driver.switch_to.window(parent_handle)
                sleep(1)

                i += 1
                continue
            while j != int(review_number):
                if j % 10 == 0:
                    sleep(2)
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located(
                        (By.XPATH, f'(//span[contains(@class,"business-review-view__body-text")])[{j+1}]')
                    )
                )

                review = driver.find_element(
                    By.XPATH, f'(//span[contains(@class,"business-review-view__body-text")])[{j + 1}]'
                )

                review.location_once_scrolled_into_view

                date = driver.find_element(
                    By.XPATH, f'(//span[contains(@class,"business-review-view__date")])[{j + 1}]'
                )

                print(review.text, date.text)

                if len(date.text.split()) > 2:
                    break

                j += 1

            driver.close()
            driver.switch_to.window(parent_handle)
            sleep(1)

            i += 1

        driver.quit()


def main():
    city = input('Область поиска: ')
    grabber = GrabberApp(city)
    grabber.grab_data()


if __name__ == '__main__':
    main()