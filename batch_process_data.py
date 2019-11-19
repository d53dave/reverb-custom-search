import psycopg2
import os
import requests
import json
import time
import arrow

from typing import List, Tuple, Dict, Any

REVERB_LISTING_API_URL = 'https://api.reverb.com/api/listings'
REVERB_API_CODE = os.getenv('REVERB_API_CODE')
PG_USER = os.getenv('PG_USER')
PG_PASS = os.getenv('PG_PASS')
PG_PORT = os.getenv('PG_PORT')
PG_HOST = os.getenv('PG_HOST')
PG_DB = os.getenv('PG_DB')

if REVERB_API_CODE is None:
    print('Cannot run batch import listing: no REVERB_API_KEY env found')
    exit(1)

req_headers = {
    'Accept-Version': '3.0',
    'Authorization': 'Bearer ' + str(REVERB_API_CODE),
    'Content-Type': 'application/hal+json'
}

insert_query = """
    INSERT INTO reverb_guitar_data.listing (id, make, model, year, condition, price_cents, currency, offers_enabled, thumbnail, full_json) VALUES %s
"""


class Listing():
    def __init__(self, values):
        self.full_json: str = json.dumps(values)

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

    print('Getting listings with params:', query)
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
        print('WARN: `humanized_params` looks wonky - ' +
              resp_json['humanized_params'])

    return list(
        map(lambda x: Listing(x),
            resp_json['listings'])), resp_json['_links']['next']['href']


def insert_listings(cur, listings: List[Listing]) -> int:
    duplicates = 0
    for listing in listings:
        try:
            cur.execute(
                'INSERT INTO listing (id, make, model, year, condition, price_cents, currency, offers_enabled, created_at, thumbnail, full_json) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);',
                (listing.id, listing.make, listing.model, listing.year,
                 listing.condition, listing.price_cents, listing.currency,
                 listing.offers_enabled, listing.created_at.datetime,
                 listing.thumbnail, listing.full_json))
        except psycopg2.IntegrityError as e:  # 23505 is constraint unique
            if e.pgcode == '23505':
                duplicates += 1
                continue
            else:
                raise e
    return duplicates


if __name__ == '__main__':
    print(arrow.utcnow(), 'Running batch reverb update')
    conn = psycopg2.connect(
        f'dbname={PG_DB} user={PG_USER} password={PG_PASS} port={PG_PORT} host={PG_HOST}'
    )
    conn.autocommit = True
    cur = conn.cursor()

    query = {
        'page': 1,
        'per_page': 50,
        'product_type': 'electric-guitars',
        'condition': 'used',
        # 'price_max': 401,
    }

    listings, next_url = get_listings_with_query(query)
    print('Processing first page')
    duplicates = insert_listings(cur, listings)
    if duplicates > 0:
        print('Found duplicates:', duplicates)
    if duplicates == 50:
        print('Exiting: Found full page of duplicates')
        exit(0)

    while next_url is not None:
        time.sleep(15)
        print('Processing url', next_url)
        listings, next_url = get_listings_for_url(next_url)
        duplicates = insert_listings(cur, listings)
        if duplicates > 0:
            print('Found duplicates:', duplicates)
        if duplicates == 50:
            print('Exiting: Found full page of duplicates')
            exit(0)
