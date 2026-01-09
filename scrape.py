from DrissionPage import ChromiumPage, ChromiumOptions
from time import sleep
import re
from urllib.parse import urlparse
import random
import pandas as pd

def random_sleep(a,b):
    sleep(random.uniform(a, b))

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
    opts = ChromiumOptions()
    website = ChromiumPage()
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
            print(infos[channel_link])
        except:
            pass
    df = pd.DataFrame(infos)
    df.to_excel("output.xlsx", index=False)
crazy()