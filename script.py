import asyncio
import aiohttp
import pandas as pd
import random
import requests
import re
import time

from tqdm import tqdm
from bs4 import BeautifulSoup


USER_AGENTS = []
USER_AGENT = None
PROXIES = []
CURRENT_PROXY = None
data = []


def count_pages() -> int:
    resp = requests.get('https://zen.yandex.ru/media/zen/channels').content
    soup = BeautifulSoup(resp, 'lxml')
    quan = int(soup.find('span', {'class':'channels-counter'}).text.split()[0])

    return quan*1000//20


def get_range() -> tuple:
    print('Выберите диапозон аудитории:')
    starts = int(input('От:'))
    to = int(input('До:'))

    return starts, to


async def get_page_html(url):
    global CURRENT_PROXY, USER_AGENT
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                return await resp.read()
    except Exception:
       change_proxy_and_ua()


async def parse_channel(content, url, starts, to):
    soup = BeautifulSoup(content, 'html.parser')

    subsribers, audience = soup.find_all('div', {'class':'desktop-channel-2-counter__value'})
    subsribers = int(subsribers.text.replace(' ', ''))
    audience = int(audience.text.replace(' ', ''))

    if not (starts<=audience and audience<=to):
        return None

    name = soup.find('div', {'class': 'desktop-channel-2-top__title _size_l'})
    if not name:
        name = soup.find('div', {'class': 'desktop-channel-2-top__title _size_s'})
    if not name:
        name = soup.find('div', {'class': 'desktop-channel-2-top__title _size_m'})

    try:
        name = name.text
    except Exception:
        name = ''

    if len(soup.find_all('span', {'class': 'zen-ui-button__content-wrapper'})) == 16:
        messages = 'Да'
    else:
        messages = 'Нет'

    social = soup.find_all('a', {'class':'desktop-channel-2-social-links__item'})
    social_networks = [soc['href'] for soc in social]

    try:
        desc = soup.find('div', {'class': 'desktop-channel-2-description'}).text
        emails = tuple(re.findall(r'[\w\.-]+@[\w\.-]+', desc))
    except Exception:
        emails = None

    if emails or social_networks or messages:
        return tuple([name, url, audience, subsribers, messages, social_networks, emails])

    return None


async def scrape_task(url, starts, to):
    global data
    content = await get_page_html(url)
    fields = await parse_channel(content, url, starts, to)
    if fields:
        data.append(fields)


async def main(urls, starts, to):
    tasks = [scrape_task(url, starts, to) for url in urls]
    await asyncio.wait(tasks)


if __name__ == '__main__':
    starts, to = get_range()

    df_list = []
    for page in tqdm(range(1, count_pages() + 1)):

        page_data = requests.get(f'http://zen.yandex.ru/media/zen/channels?page={page}').content
        soup = BeautifulSoup(page_data, 'lxml')

        urls = ['http://zen.yandex.ru'+div['href'] for div in soup.find_all('a', {'class':'channel-item__link'})]

        asyncio.run(main(urls, starts, to))

        df = pd.DataFrame(data)
        df_list.append(df)
        pd.concat(df_list).to_excel('results.xlsx', index=False)
        data = []
