import psycopg2
import os
import requests
import json
import time
import arrow
import logging

from typing import List, Tuple, Dict, Any
from collections import defaultdict
from apscheduler.schedulers.blocking import BlockingScheduler

from telegram_bot import Bot

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                    level=logging.INFO)

REVERB_LISTING_API_URL = 'https://api.reverb.com/api/listings'
REVERB_API_CODE = os.getenv('REVERB_API_CODE')
PG_USER = os.getenv('PG_USER')
PG_PASS = os.getenv('PG_PASS')
PG_PORT = os.getenv('PG_PORT')
PG_HOST = os.getenv('PG_HOST')
PG_DB = os.getenv('PG_DB')

if REVERB_API_CODE is None:
    logging.error(
        'Cannot run batch import listing: no REVERB_API_KEY env found')
    exit(1)

req_headers = {
    'Accept-Version': '3.0',
    'Authorization': 'Bearer ' + str(REVERB_API_CODE),
    'Content-Type': 'application/hal+json'
}

insert_query = """
    INSERT INTO reverb_guitar_data.listing (id, make, model, year, condition, price_cents, currency, offers_enabled, thumbnail, full_json) VALUES %s
"""

conn = psycopg2.connect(
    f'dbname={PG_DB} user={PG_USER} password={PG_PASS} port={PG_PORT} host={PG_HOST}'
)
conn.autocommit = True
cur = conn.cursor()

bot = Bot(str(os.getenv('TELEGRAM_TOKEN')))

sched = BlockingScheduler()


class Listing():
    def __init__(self, values):
        self.full_json: Dict = values

        self.id: int = int(values['id'])
        self.make: str = values['make']
        self.model: str = values['model']
        self.year: str = values['year']
        self.condition: str = values['condition']['display_name']
        self.price_cents: int = values['buyer_price']['amount_cents']
        self.currency: str = values['buyer_price']['currency']
        self.offers_enabled: bool = values['offers_enabled']
        self.created_at: arrow.Arrow = arrow.get(values['created_at'])
        self.thumbnail: str = values['photos'][0]['_links']['thumbnail'][
            'href']


def get_listings_with_query(query: Dict[str, Any]
                            ) -> Tuple[List[Listing], str]:

    logging.info('Getting listings with params: %s', query)
    response = requests.get(REVERB_LISTING_API_URL,
                            params=query,
                            headers=req_headers)
    resp_json = response.json()

    return list(
        map(lambda x: Listing(x),
            resp_json['listings'])), resp_json['_links']['next']['href']


def get_listings_for_url(url) -> Tuple[List[Listing], str]:
    response = requests.get(url, headers=req_headers)
    resp_json = response.json()

    if 'Used Electric Guitars' not in resp_json['humanized_params']:
        logging.warn('`humanized_params` looks wonky - %s',
                     resp_json['humanized_params'])

    return list(
        map(lambda x: Listing(x),
            resp_json['listings'])), resp_json['_links']['next']['href']


def insert_listings(cur, listings: List[Listing]
                    ) -> Tuple[List[Listing], List[Listing]]:
    duplicates = []
    inserted = []
    for listing in listings:
        try:
            cur.execute(
                'INSERT INTO listing (id, make, model, year, condition, price_cents, currency, offers_enabled, created_at, thumbnail, full_json) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);',
                (listing.id, listing.make, listing.model, listing.year,
                 listing.condition, listing.price_cents, listing.currency,
                 listing.offers_enabled, listing.created_at.datetime,
                 listing.thumbnail, json.dumps(listing.full_json)))
            inserted.append(listing)
        except psycopg2.IntegrityError as e:  # 23505 is constraint unique
            if e.pgcode == '23505':
                duplicates.append(listing)
                continue
            else:
                raise e
    return inserted, duplicates


def process_bot_notifications(bot: Bot, listings: List[Listing]) -> None:
    bot_terms = bot.get_terms()
    all_results: Dict[str, List[Listing]] = defaultdict(list)
    for listing in listings:
        for term in bot_terms:
            lower_term = term.lower()
            if lower_term in listing.full_json['title'].lower(
            ) or lower_term in listing.model.lower(
            ) or lower_term in listing.make.lower(
            ) or lower_term in listing.full_json['description'].lower():
                all_results[term].append(listing)

    for term, listings in all_results.items():
        strings = list(
            map(
                lambda lst:
                f"[{lst.full_json['title']}]({lst.full_json['_links']['web']['href']})",
                listings))
        bot.send_update(term, strings)


@sched.scheduled_job('cron', minute='*/7')
def update_listings():
    logging.info('Running batch reverb update')
    query = {
        'page': 1,
        'per_page': 50,
        'product_type': 'electric-guitars',
        'condition': 'used',
        # 'price_max': 401,
    }

    listings, next_url = get_listings_with_query(query)

    logging.info('Processing first page')
    inserted, duplicates = insert_listings(cur, listings)

    process_bot_notifications(bot, inserted)

    if len(duplicates) > 0:
        logging.info('Found duplicates: %d', len(duplicates))
    if len(inserted) == 0:
        logging.info('Exiting: Found full page of duplicates')
        return

    while next_url is not None:
        time.sleep(10)
        logging.info('Processing url %s', next_url)
        listings, next_url = get_listings_for_url(next_url)
        inserted, duplicates = insert_listings(cur, listings)

        process_bot_notifications(bot, inserted)

        if len(duplicates) > 0:
            logging.info('Found duplicates: %d', len(duplicates))
        if len(inserted) == 0:
            logging.info('Exiting: Found full page of duplicates')
            return


if __name__ == '__main__':
    try:
        sched.start()
    except KeyboardInterrupt:
        logging.info('Got SIGTERM! Terminating...')
