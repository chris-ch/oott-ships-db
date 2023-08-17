import argparse
import csv
import logging
import os
from string import Template
from typing import Optional

from bs4 import BeautifulSoup

from webscrapetools.urlcaching import set_cache_path, open_url, invalidate_key

_VESSEL_TYPES = {
    'All Cargos': '4',
    'All Tankers': '6',
    'Crude Oil Tankers': 601
}

_URL_BASE = 'https://www.vesselfinder.com'
_URL_SEARCH_TEMPLATE = Template(_URL_BASE + '/vessels?type=$vessel_type&page=$page_count')
_URL_INDEX = f'{_URL_BASE}/vessels'


def load_index():
    html_text = open_url(_URL_INDEX, throttle=1)
    html = BeautifulSoup(html_text, 'html.parser')
    vessel_types = {int(option['value']): option.text for option in sorted(html.find(id='advsearch-ship-type').find_all('option'), key=lambda item:int(item['value']))}
    for vessel_type_code, vessel_type_name in vessel_types.items():
        print(f'- {vessel_type_code} : {vessel_type_name}')


def load_page(vessel_type_code: int, page_current: int, page_max: int = None):
    if page_current <= 0:
        raise IndexError(f'invalid page index {page_current}')

    url = _URL_SEARCH_TEMPLATE.substitute({'vessel_type': vessel_type_code, 'page_count': page_current})
    html_text = open_url(url, throttle=1)
    try:
        html = BeautifulSoup(html_text, 'html.parser')
        ships = html.find('table', {'class': 'results'})
        page_content = list()
        for ship_row in ships.find('tbody').find_all('tr'):
            row_data = dict()
            field1 = ship_row.find('td', {'class': 'v2'})
            flag = field1.find('div', {'class': 'flag-icon'})
            if flag:
                row_data['country'] = flag.get('title').strip()

            vessel_name = field1.find('div', {'class': 'slna'})
            if vessel_name:
                row_data['name'] = vessel_name.text.strip()

            vessel_type = field1.find('div', {'class': 'slty'})
            if vessel_type:
                row_data['type'] = vessel_type.text.strip()

            field2 = ship_row.find('td', {'class': 'v3'})
            field3 = ship_row.find('td', {'class': 'v4'})
            field4 = ship_row.find('td', {'class': 'v5'})
            field5 = ship_row.find('td', {'class': 'v6'})
            if field1:
                row_data['imo'] = field1.find('a').get('href').split('/')[-1]

            if field2 and field2.text.strip().replace(',', '').isdigit():
                row_data['year-built'] = int(field2.text.strip().replace(',', ''))

            if field3 and field3.text.strip().replace(',', '').isdigit():
                row_data['gross-tons'] = int(field3.text.strip().replace(',', ''))

            if field4 and field4.text.strip().replace(',', '').isdigit():
                row_data['dead-weight-tons'] = int(field4.text.strip().replace(',', ''))

            if field5 and '/' in field5.text:
                length, width = field5.text.split('/')
                if length.strip().replace(',', '').isdigit():
                    row_data['length_meters'] = int(length.strip().replace(',', ''))
                if width.strip().replace(',', '').isdigit():
                    row_data['width_meters'] = int(width.strip().replace(',', ''))

            page_content.append(row_data)

        pagination = html.find('div', {'class': 'pagination-controls'}).find('span')
        completed = True
        if pagination and '/' in pagination.text:
            _, total = pagination.text.split('/')
            if total.strip().replace(',', '').isdigit():
                page_last = int(total.strip().replace(',', ''))
                logging.info('processed page %s (last: %s, max: %s)', page_current, page_last, page_max)
                completed = page_current >= page_last or page_current >= (page_max, page_current + 1)[page_max is None]

    except Exception:
        logging.exception('failed to load page %s', page_current)
        invalidate_key(url)
        raise

    return page_content, completed


def load_pages(vessel_type_code: int, output_dir: str, page_max: Optional[int] = None, page_start: int = 1):
    page_counter = page_start
    results, completed = load_page(vessel_type_code, page_counter, page_max=page_max)
    while not completed:
        page_counter += 1
        page_results, completed = load_page(vessel_type_code, page_counter, page_max=page_max)
        results += page_results

    with open(os.path.sep.join([output_dir, f'ship-db-{vessel_type_code}.csv']), 'w', encoding='utf-8') as ship_db:
        csv_writer = csv.DictWriter(ship_db, sorted(results[0].keys()))
        csv_writer.writeheader()
        csv_writer.writerows(results)


def main(args):
    if args.list_vessel_types:
        load_index()

    else:
        if not os.path.exists(args.output_dir):
            logging.info(f'creating output directory "{os.path.abspath(args.output_dir)}"')
            os.makedirs(args.output_dir)

        if len(args.vessel_type_codes) == 0:
            logging.warning('no vessel type code specified')

        for code in set(args.vessel_type_codes):
            load_pages(code, args.output_dir, page_max=None, page_start=1)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    logging.getLogger('requests').setLevel(logging.WARNING)
    file_handler = logging.FileHandler('output/download-vessels.log', mode='w')
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)

    parser = argparse.ArgumentParser(description='Importing vessels from online DB',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter
                                     )

    parser.add_argument('--list-vessel-types', action='store_true', help='only displays available vessel types')
    parser.add_argument('--output-dir', type=str, help='location of output directory', default='.')
    parser.add_argument('--output_file', type=str, help='name of the output CSV file', default='vessels-<type code>.csv')
    parser.add_argument('vessel_type_codes', type=int, nargs='*', help='codes of the vessel type')
    args = parser.parse_args()

    set_cache_path(os.path.sep.join([args.output_dir, 'urlcaching']))

    try:
        main(args)

    except:
        logging.exception('uncaught error')

