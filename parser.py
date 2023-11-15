from datetime import datetime
import requests
from bs4 import BeautifulSoup
from multiprocessing import Pool
import csv
from config import CSV_FILE_NAME


def get_html(URL): # делать запрос по ссылке и возвращать html код этой страницы
    response = requests.get(URL)
    return response.text


def get_posts_links(html):
    links = []
    soup = BeautifulSoup(html, "html.parser")
    table_data = soup.find("div", {"class":"listings-wrapper"})
    data = table_data.find_all("div", {"class":"main-wrapper"})
    for p in data:
        href = p.find('a').get('href')
        full_url = 'https://www.house.kg' + href
        links.append(full_url)
    return links # возвращает ссылки на детальную страницу постов


def get_detail_post(html, post_url):
    soup = BeautifulSoup(html, 'html.parser')
    content = soup.find('div', {'class':'content-wrapper'})
    detail = content.find('div', {'class':'main-content'})
    title = detail.find('div', {'details-header'}).find('h1').text.strip()
    price_som = detail.find('div', {'prices-block'}).find('div', {'price-som'}).text.strip()
    price_dollar = detail.find('div', {'prices-block'}).find('div', {'price-dollar'}).text.strip()
    phone = detail.find('div', {'phone-fixable-block'}).find('div', {'number'}).text.strip()
    try:
        description = detail.find('div', {'description'}).find('p').text.strip()
    except AttributeError:
        description = 'ОПИСАНИЕ ОТСУТСТВУЕТ'
    price_som = int(price_som.replace("сом", "").strip().replace(" ", ""))
    price_dollar = int(price_dollar.replace("$", "").strip().replace(" ", ""))

    data = {
        'title': title,
        'som':  price_som,
        'dollar': price_dollar,
        'mobile': phone,
        'description': description,
        'link': post_url
    }
    print(data)
    return data


def get_lp_number(html):
    soup = BeautifulSoup(html, 'html.parser')
    content = soup.find('nav')
    ul = content.find('ul', {'class':'pagination'})
    lp = ul.find_all('a', {'class':'page-link'})[-1]
    n = lp.get('data-page')
    return int(n)


def write_data(data):
    # Запись данных
    with open(CSV_FILE_NAME, 'a', encoding='utf-8') as file:
        headers = ['title', 'mobile', 'dollar', 'som', 'link', 'description']
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writerow(data)


def write_header_csv():
    # Запись заголовка
    with open(CSV_FILE_NAME, 'w', encoding='utf-8') as file:
        headers = ['title', 'mobile', 'dollar', 'som', 'link', 'description']
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()


def get_parse_page(page):
    URL_MAIN = 'https://www.house.kg/'
    filter = 'kupit?sort_by=upped_at+desc'
    FULL_URL = URL_MAIN + filter
    print(f'Парсинг страницы: {page}')
    FULL_URL += f'&page={page}'
    html = get_html(FULL_URL)
    post_links = get_posts_links(html)
    for link in post_links:
        post_html = get_html(link)
        post_data = get_detail_post(post_html, post_url=link)
        write_data(data=post_data)


def main():
    write_header_csv()
    start = datetime.now()
    URL_MAIN = 'https://www.house.kg/'
    filter = 'kupit?sort_by=upped_at+desc'
    FULL_URL = URL_MAIN + filter
    last_page = get_lp_number(get_html(FULL_URL))
    with Pool(5) as p:
        p.map(get_parse_page, range(1, last_page+1))

    end = datetime.now()
    print('Время выполнения: ', end-start)


if __name__ == '__main__':
    main()
