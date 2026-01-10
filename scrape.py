from DrissionPage import ChromiumPage, ChromiumOptions
from time import sleep
import re
from urllib.parse import urlparse
import random
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s - %(message)s')


def random_sleep(a, b):
    t = random.uniform(a, b)
    logging.info(f"sleep {t:.2f}s")
    sleep(t)

def extract_emails(text):
    # Регулярное выражение для поиска стандартных email-адресов
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    
    # Находим все совпадения
    emails = re.findall(email_pattern, text)
    
    # Убираем дубликаты, если они есть
    return list(set(emails))

def extract_instagram_links(text: str) -> list:
    pattern = r'(?:https?:\/\/)?(?:www\.)?instagram\.com\/[A-Za-z0-9._]+'
    return re.findall(pattern, text)

def parse_number(text: str) -> int:
    """
    Преобразует:
    - '42,6 тыс. подписчиков' -> 42600
    - '3 734 972 просмотра' -> 3734972
    - '76 видео' -> 76
    """
    text = text.lower()

    # тыс.
    if 'тыс' in text:
        num = re.search(r'([\d,\.]+)', text)
        if num:
            return int(float(num.group(1).replace(',', '.')) * 1000)

    # млн (на будущее)
    if 'млн' in text:
        num = re.search(r'([\d,\.]+)', text)
        if num:
            return int(float(num.group(1).replace(',', '.')) * 1_000_000)

    # обычные числа с пробелами
    num = re.search(r'([\d\s]+)', text)
    if num:
        return int(num.group(1).replace(' ', ''))

    return 0

def extract_metrics(info):
    country = info[11].text.strip()
    channel_link = info[9].text.strip()

    subs = parse_number(info[15].text)
    videos = parse_number(info[17].text)
    views = parse_number(info[19].text)

    return {
        "country": country,
        "channel_link": channel_link,
        "subscribers": subs,
        "videos": videos,
        "views": views
    }

def crazy():
    logging.info("Init Chromium options")
    opts = ChromiumOptions()

    opts.set_argument("--no-sandbox")
    opts.set_argument("--disable-dev-shm-usage")
    opts.incognito()
    opts.headless()
    opts.no_imgs(True).mute(True)

    # Явно указываем путь к бинарнику Chromium внутри контейнера, если метод доступен
    try:
        opts.set_browser_path('/usr/bin/chromium')
    except Exception:
        pass

    logging.info("Start ChromiumPage")
    website = ChromiumPage(opts)
    
    logging.info("Open search results page")
    website.get("https://www.youtube.com/results?search_query=faceless+youtube+tutorial")
    random_sleep(5,10)
    for i in range(10):
        website.scroll.to_bottom()
        random_sleep(1,5)
    links = []
    lst = website.eles("tag:a@id=thumbnail")
    for i in lst:
        attr = i.attr("href")
        if attr:
            links.append(attr)
    logging.info(f"Collected {len(links)} video links")

    infos = dict()
    for i in links:
        try:
            contact_info = ""
            country = ""
            channel_link = ""
            subscribers = 0
            videos = 0
            views = 0
            website.get(i)
            random_sleep(2,5)
            if website.url.split("/")[3] == "shorts":
                try:
                    website.get(website.ele("@class=ytReelMultiFormatLinkViewModelEndpoint").attr("href"))
                    website.refresh()
                except:
                    continue

            random_sleep(5,10)
            try:
                info = website.ele("@class=style-scope ytd-video-renderer")
                description = website.ele("@id=bottom-row").text
                contact_info = extract_emails(description)
                if not contact_info:
                    contact_info = extract_instagram_links(description)
            except:
                pass
            website.ele("@class=style-scope ytd-channel-name complex-string").click()
            website.refresh()
            random_sleep(5,10)
            website.ele("@class=ytDescriptionPreviewViewModelHost yt-page-header-view-model__page-header-description ytDescriptionPreviewViewModelClickable").click()
            if not contact_info:
                contact_info = extract_instagram_links(website.ele("@id=description-container").text)
            
            info = website.ele("@id=additional-info-container").eles("tag:td@class=style-scope ytd-about-channel-renderer")
            country = info[11].text.strip()
            channel_link = info[9].text.strip()
            subscribers = parse_number(info[15].text)
            videos = parse_number(info[17].text)
            views = parse_number(info[19].text)
            infos[channel_link] = {
                "contact_info": contact_info,
                "country": country,
                "subscribers": subscribers,
                "videos": videos,
                "views": views
            }
            logging.info(str(infos[channel_link]))
        except:
            pass
    # Приводим словарь к табличному виду: строки — каналы, столбцы — метрики
    df = pd.DataFrame.from_dict(infos, orient='index').reset_index().rename(columns={'index': 'channel_link'})
    # Нормализуем contact_info (список → строка с разделителем ';')
    if 'contact_info' in df.columns:
        df['contact_info'] = df['contact_info'].apply(lambda x: ';'.join(x) if isinstance(x, list) else str(x))
    logging.info(f"Saving {len(df)} rows to output.csv")
    df.to_csv("output.csv", index=False)
crazy()