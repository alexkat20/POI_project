import time

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
import pandas as pd


class GrabberApp:

    def __init__(self, city):
        self.city = city
        self.df = pd.DataFrame({"place": [], "review": [], "date": []})

        # chrome_options.add_argument("--no-sandbox")
        # chrome_options.add_argument("--window-size=1420,1080")

        # chrome_options.add_argument("--disable-gpu")

        options = webdriver.ChromeOptions()
        #  options.add_argument("--headless")
        options.headless = False
        options.page_load_strategy = "none"

        chrome_path = ChromeDriverManager().install()
        chrome_service = Service(chrome_path)
        self.driver = Chrome(options=options, service=chrome_service)

    def wait_for_presence(self, item):
        for i in range(10):
            try:
                WebDriverWait(self.driver, 3).until(
                    EC.presence_of_element_located(
                        (By.XPATH, item)
                    )
                )
            except:
                continue


    def grab_data(self):
        #  self.driver.maximize_window()
        self.driver.get('https://yandex.ru/maps')

        self.wait_for_presence('(//input[contains(@class,"input__control _bold")])')

        # Вводим данные поиска
        self.driver.find_element(
            By.XPATH, '(//input[contains(@class,"input__control _bold")])'
        ).send_keys(self.city)

        self.wait_for_presence('(//div[contains(@class,"small-search-form-view__icon _type_search")])')
        # Нажимаем на кнопку поиска
        self.driver.find_element(
            By.XPATH, f'(//div[contains(@class,"small-search-form-view__icon _type_search")])'
        ).click()
        self.wait_for_presence('(//div[contains(@class,"tabs-select-view__title _name_inside")])')

        try:
            self.driver.find_element(
                By.XPATH, f'(//div[contains(@class,"tabs-select-view__title _name_inside")])'
            ).click()
        except:
            self.driver.quit()

        parent_handle = self.driver.window_handles[0]
        i = 2

        while i:
            url = None

            try:
                self.wait_for_presence(f'(//div[contains(@class,"search-business-snippet-view__content")])[{i}]')
                org_info = self.driver.find_element(
                    By.XPATH, f'(//div[contains(@class,"search-business-snippet-view__content")])[{i}]'
                )
                elements = org_info.find_elements(By.TAG_NAME, "a")
                for el in elements:
                    link = el.get_attribute("href")
                    if "review" in link:
                        url = link
                print(url)
                org_info.location_once_scrolled_into_view
                if not url:
                    i += 1
                    continue
                self.driver.execute_script(f'window.open("{url}","org_tab");')

                child_handle = [x for x in self.driver.window_handles if x != parent_handle][0]
                self.driver.switch_to.window(child_handle)

                try:

                    self.wait_for_presence(
                        '(//div[contains(@class,"rating-ranking-view")])')
                    self.driver.find_element(
                        By.XPATH, f'(//div[contains(@class,"rating-ranking-view")])'
                    ).click()
                    self.driver.find_element(
                        By.XPATH, f'(//div[contains(@class,"rating-ranking-view__popup-line")])[2]'
                    ).click()

                    self.wait_for_presence('(//div[contains(@class,"tabs-select-view__title _name_reviews _selected")])')

                    review_number = self.driver.find_element(
                        By.XPATH, f'(//div[contains(@class,"tabs-select-view__title _name_reviews _selected")])'
                    ).get_attribute("aria-label").split(", ")[1]

                    j = 0
                except:
                    self.driver.close()
                    self.driver.switch_to.window(parent_handle)

                    i += 1
                    continue
                self.wait_for_presence(f'(//h1[contains(@class,"orgpage-header-view__header")])')

                place = self.driver.find_element(
                    By.XPATH, f'(//h1[contains(@class,"orgpage-header-view__header")])'
                )
                print(place.text)
                while j != int(review_number):
                    if j % 20 == 0:
                        sleep(2)

                    self.wait_for_presence(f'(//span[contains(@class,"business-review-view__body-text")])[{j + 1}]')

                    review = self.driver.find_element(
                        By.XPATH, f'(//span[contains(@class,"business-review-view__body-text")])[{j + 1}]'
                    )

                    review.location_once_scrolled_into_view

                    date = self.driver.find_element(
                        By.XPATH, f'(//span[contains(@class,"business-review-view__date")])[{j + 1}]'
                    )

                    #  print(review.text, date.text)
                    self.df.loc[len(self.df)] = {"place": place.text, "review": review.text, "date": date.text}

                    if len(date.text.split()) > 2:
                        i += 1
                        break

                    j += 1
                print("--------------------------------------------------------------------")

                self.driver.close()
                self.driver.switch_to.window(parent_handle)

                i += 1

            except:
                break

        self.driver.quit()
        return self.df


def main():
    city = input('Область поиска: ')
    grabber = GrabberApp(city)
    data = grabber.grab_data()
    data.to_csv("data.csv")


if __name__ == '__main__':
    main()
