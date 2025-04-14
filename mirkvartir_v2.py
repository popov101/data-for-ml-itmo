import requests
import pandas as pd
from tqdm import tqdm
import time
import random
import numpy as np

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json;charset=UTF-8",
    "Origin": "https://arenda.mirkvartir.ru",
    "Referer": "https://arenda.mirkvartir.ru/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 YaBrowser/25.2.0.0 Safari/537.36",
    "Accept-Language": "ru-RU,ru;q=0.9",
}

API_URL = "https://www.mirkvartir.ru/listingapi/ListingPageModel/"

def make_payload(page):
    return {
        "locationIds": "MK_Region|78",
        "site": 16,
        "p": page,
        "pricePeriod": 4,
        "types": "1,2",
        "subwayDistanceType": 1,
        "activeProperties": [],
        "groupField": None,
    }

def extract_offer_data(offer):
    loc = offer.get('locationInfo', {}) or {}
    coords = loc.get('coordinate', {}) or {}
    subway = loc.get('subwayInfo', {}) or {}
    contact = offer.get('listingOfferContact', {}) or {}
    addr = loc.get('addresses', []) or []

    return {
        "id": offer.get("id"),
        "url": offer.get("url"),
        "title": offer.get("title"),
        "description": offer.get("description", "").strip().replace("\n", " "),
        "price": offer.get("price"),
        "latitude": coords.get("lat"),
        "longitude": coords.get("lon"),
        "metro_name": subway.get("subwayName"),
        "address": ", ".join([a.get("name", "") for a in addr]),
        "agency_name": contact.get("name"),
        "photos_count": len(offer.get("photos", [])),
        "photos" : offer.get("photos", []),
        "main_photo_url": offer.get("photos")[0] if offer.get("photos") else None,
        "update_time": offer.get("updateTime"),
        "source": "mirkvartir",
        "page_number": offer.get("_page")
    }

def collect_offers(max_pages=100, min_delay=2, max_delay=5):
    all_data = []
    seen_ids = set()
    session = requests.Session()
    session.headers.update(HEADERS)
    
    try:
        initial_response = session.post(API_URL, json=make_payload(1))
        initial_data = initial_response.json()
        total_offers = initial_data.get("listingModel", {}).get("totalOffers", 0)
        print(f"Всего найдено объявлений: {total_offers}")
    except Exception as e:
        print(f"[!] Ошибка при проверке общего количества объявлений: {e}")
        total_offers = 0

    pages = list(range(1, max_pages + 1))
    random.shuffle(pages)
    processed_pages = set()
    
    with tqdm(total=max_pages, desc="Сбор данных") as pbar:
        for page in pages:
            if page in processed_pages:
                continue
                
            payload = make_payload(page)
            try:
                response = session.post(API_URL, json=payload, timeout=15)
                response.raise_for_status()
                
                data = response.json()
                listing_model = data.get("listingModel", {})
                offers = listing_model.get("offers", [])
                
                if not offers:
                    print(f"\n[!] Страница {page} пустая, пропускаем")
                    processed_pages.add(page)
                    pbar.update(1)
                    continue
                    
                for offer in offers:
                    offer["_page"] = page
                
                page_data = []
                duplicates_count = 0
                for offer in offers:
                    parsed = extract_offer_data(offer)
                    if parsed and parsed['id'] not in seen_ids:
                        seen_ids.add(parsed['id'])
                        page_data.append(parsed)
                    else:
                        duplicates_count += 1
                
                all_data.extend(page_data)
                processed_pages.add(page)
                pbar.update(1)
                
                print(f"\n[+] Страница {page}: собрано {len(page_data)} объявлений, пропущено дубликатов: {duplicates_count}")
                
                delay = random.uniform(min_delay, max_delay)
                time.sleep(delay)
                
                if len(processed_pages) % 5 == 0:
                    extra_delay = random.uniform(2, 7)
                    print(f"\n[Пауза] Дополнительная задержка {extra_delay:.1f} сек")
                    time.sleep(extra_delay)
                    
            except requests.exceptions.RequestException as e:
                print(f"\n[!] Ошибка сети на странице {page}: {e}")
                time.sleep(10)
            except Exception as e:
                print(f"\n[!] Ошибка на странице {page}: {e}")
                time.sleep(5)
                
            if len(processed_pages) >= max_pages:
                break
                
    return all_data

if __name__ == "__main__":
    MAX_PAGES = 200
    MIN_DELAY = 2
    MAX_DELAY = 5
    
    print("Начало сбора данных со случайным порядком страниц...")
    offers_data = collect_offers(max_pages=MAX_PAGES, min_delay=MIN_DELAY, max_delay=MAX_DELAY)
    
    if offers_data:
        df = pd.DataFrame(offers_data)
        
        # Проверка и удаление дубликатов
        initial_len = len(df)
        df = df.drop_duplicates(subset=['id'])
        duplicates_removed = initial_len - len(df)
        
        if 'page_number' in df.columns:
            df.drop('page_number', axis=1, inplace=True)
        
        filename = "mirkvartir_random_pages_listings.csv"
        df.to_csv(filename, index=False)
        print(f"\nСобрано и сохранено {len(df)} объявлений в {filename}")
        print(f"Удалено дубликатов при финальной проверке: {duplicates_removed}")
        
        # Получаем количество уникальных страниц из самих данных
        unique_pages = len(df['page_number'].unique())
        print(f"Обработано уникальных страниц: {unique_pages}")
    else:
        print("\nНе удалось собрать данные. Проверьте соединение и параметры запроса.")
        