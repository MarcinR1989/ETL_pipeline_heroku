from time import sleep
from os import listdir
import os
import google_bigquery
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.by import By
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.ui import WebDriverWait

wait_time = 100


def selecting_date(calendar_css_sel, calendar_left_button,
                   month_name_css_sel, day_css_sel, download_css_sel,
                   driver):
    driver.find_element_by_css_selector(calendar_css_sel).click()
    calendar_left = driver.find_element_by_css_selector(
        calendar_left_button)
    while True:
        calendar_left.click()
        month_name = driver.find_element_by_css_selector(
            month_name_css_sel).get_attribute('innerText')
        if month_name == 'Kwiecień 2020' or month_name == 'April 2020':
            driver.find_element_by_css_selector(day_css_sel).click()
            driver.find_element_by_css_selector(download_css_sel).click()
            break


def download_reports(download_dir):
    options = webdriver.ChromeOptions()
    preferences = {"download.default_directory": download_dir,
                   "directory_upgrade": True,
                   "safebrowsing.enabled": True}
    options.add_experimental_option("prefs", preferences)
    driver = webdriver.Chrome(
        executable_path=os.environ.get("CHROMEDRIVER_PATH"),
        chrome_options=options)

    window_h = 1853
    window_w = 1167
    driver.set_window_size(window_h / 2, window_h)
    driver.set_window_position(window_h / 2 + 67, 0)

    driver.get('https://charliefoodandfriends.masterlifecrm.com/reports/excel')
    print(f'Site title {driver.title}')

    select_language = Select(driver.find_element_by_css_selector('#app > div > div > div.login-header > select'))
    select_language.select_by_value('PL')
    login_input = driver.find_elements_by_tag_name('input')
    login_input[0].send_keys(os.environ.get("AUTH_LOGIN"))
    login_input[1].send_keys(os.environ.get("AUTH_PASSWORD"))
    driver.find_element_by_tag_name('button').click()

    try:
        WebDriverWait(driver, wait_time).until(
            ec.title_contains('charliefoodandfriends'))
        print(driver.title)
    except TimeoutException:
        print('Title not found.')
        driver.close()

    # Navigate to "Raporty zbiorcze" tab
    driver.find_element_by_css_selector('a[href^="/reports/excel"]').click()

    # 1.Download "Wszystkie zamówienia"
    wszystkie_zam_download_but = 'div.col-auto:nth-child(1) ' \
                                 '> div:nth-child(1) > button:nth-child(2)'
    driver.find_element_by_css_selector(wszystkie_zam_download_but).click()

    try:
        WebDriverWait(driver, wait_time).until(ec.element_to_be_clickable(
            (By.CSS_SELECTOR, wszystkie_zam_download_but)))
        print('1.Downloaded: raport-wszystkich-zamowien')
    except TimeoutException:
        print('1.NOT downloaded: raport-wszystkich-zamowien')
        driver.close()

    onlyfiles = [f for f in listdir(download_dir)]
    print(onlyfiles)

    # 2.Download "Finanse"
    finanse_download_but = \
        'div.col-auto:nth-child(6) > div:nth-child(1) > button:nth-child(3)'
    selecting_date(
        'div.col-auto:nth-child(6) > div:nth-child(1) > div:nth-child(2) '
        '> div:nth-child(1) > div:nth-child(1) > div:nth-child(1) '
        '> div:nth-child(1) > input:nth-child(1)',
        '.is-open > div:nth-child(1) > div:nth-child(1) > i:nth-child(1)',
        '.is-open > div:nth-child(1) > div:nth-child(1) '
        '> h3:nth-child(2) > span:nth-child(1)',
        '.is-open > div:nth-child(1) > div:nth-child(3) > div:nth-child(3)',
        finanse_download_but, driver)

    try:
        WebDriverWait(driver, wait_time).until(ec.element_to_be_clickable(
            (By.CSS_SELECTOR, finanse_download_but)))
        print('2.Downloaded: raport-finansow-od-2020-04-01-do-2021-01-28')
    except TimeoutException:
        print('2.NOT downloaded: raport-finansow-od-2020-04-01-do-2021-01-28')
        driver.close()

    onlyfiles = [f for f in listdir(download_dir)]
    print(onlyfiles)

    # Navigate to "Raporty zbiorcze" tab
    driver.find_element_by_css_selector(
        'a[href^="/reports/current-payments"]').click()
    # 3.Download "Raport bieżacych płatności"
    platnosci_download_but = 'div.controls:nth-child(1) > button:nth-child(3)'
    selecting_date(
        '.date-picker-report-date-from > div:nth-child(1) '
        '> div:nth-child(1) > input:nth-child(1)',
        '.is-open > div:nth-child(1) > div:nth-child(1) > i:nth-child(1)',
        '.is-open > div:nth-child(1) > div:nth-child(1) > h3:nth-child(2) '
        '> span:nth-child(1)',
        '.is-open > div:nth-child(1) > div:nth-child(3) > div:nth-child(3)',
        platnosci_download_but, driver)
    try:
        WebDriverWait(driver, wait_time).until(ec.element_to_be_clickable(
            (By.CSS_SELECTOR, platnosci_download_but)))
        print('3.Downloaded: Płatności-01-04-2020-29-01-2021')
    except TimeoutException:
        print('3.NOT downloaded: Płatności-01-04-2020-29-01-2021')
        driver.close()
    driver.find_element_by_css_selector('#test-table-xls-button').click()

    sleep(5)
    onlyfiles = [f for f in listdir(download_dir)]
    print(onlyfiles)


def del_files(download_dir):
    try:
        [os.remove(os.path.join(download_dir, f))
         for f in os.listdir(download_dir)]
        print(f'Files from {download_dir} has been deleted.')
    except FileNotFoundError:
        print(f'There are no files in {download_dir} folder.')


def execute_etl_pipeline(download_dir):
    del_files(download_dir)
    download_reports(download_dir)
    google_bigquery.xls_to_csv(download_dir)
    google_bigquery.bigquery_connect(download_dir)


if __name__ == '__main__':
    # execute_etl_pipeline(os.environ.get("DOWNLOAD_DIR"))
    for n in range(5):
        try:
            execute_etl_pipeline(os.environ.get("DOWNLOAD_DIR"))
        except:
            print(f"SOMETHING WENT WRONG. I'LL TRY AGAIN. TRIES: {n}")
        else:
            print(f"UPLOAD COMPLETED! (in {n} run)")
            break
