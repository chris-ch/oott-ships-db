import argparse
import csv
import logging
import os
from string import Template

from bs4 import BeautifulSoup

from urlcaching import set_cache_path, open_url, invalidate_key

_VESSEL_TYPES = {
    'Cargo ships': '4',
    'Tanker': '6'
}

_URL_BASE = 'https://www.vesselfinder.com'
_URL_LIST_TEMPLATE = Template(_URL_BASE + '/vessels?t=$vessel_type&page=$page_count')


def load_page(page_current, page_max=None):
    assert page_current > 0
    vessel_type = 'Tanker'
    url = _URL_LIST_TEMPLATE.substitute({'vessel_type': _VESSEL_TYPES[vessel_type], 'page_count': page_current})
    html_text = open_url(url)
    try:
        html = BeautifulSoup(html_text, 'html.parser')
        ship_rows = html.find_all('div', {'class': 'ship-row-details'})
        page_content = list()
        for ship_row in ship_rows:
            ship_header = ship_row.find_next('header')
            ship_details_url_path = ship_header.find_next('a', {'rel': 'bookmark'})['href']
            mmsi_lookup = ship_details_url_path.split('-MMSI-')
            ship_MMSI = ''
            if len(mmsi_lookup) > 1:
                ship_MMSI = mmsi_lookup[-1]

            ship_country_owner_a_tag = ship_header.find_next('a', {'rel': 'bookmark'})
            ship_country_owner_img_tag = ship_country_owner_a_tag.find_next('img', {'class': 'ship-flag'})
            ship_country_owner = ''
            if ship_country_owner_img_tag:
                ship_country_owner = ship_country_owner_img_tag['title'][len('Flag of') + 1:]

            ship_name = ship_header.find_next('a', {'rel': 'bookmark'}).text.strip()
            row_data = {'ship_name': ship_name, 'ship_country_owner': ship_country_owner,
                        'ship_details_url_path': ship_details_url_path, 'ship_MMSI': ship_MMSI}

            for row_param in ship_row.find_all('div', {'class': 'row param'}):
                param_name = row_param.find_next('div')
                param_value_raw = param_name.find_next('div')
                if param_value_raw.text.strip().upper() == 'N/A':
                    param_value = ''

                else:
                    param_value = param_value_raw.text

                row_data[param_name.text] = param_value

            page_content.append(row_data)

        pagination = html.find('div', {'id': 'vessels-list'}).find('ul', {'class': 'mypagination'})
        page_last = pagination.find_next('li', {'class': 'last'}).find_next('a')['href'].split('&page=')[-1]
        logging.info('processed page %s (last: %s, max: %s)', page_current, page_last, page_max)
        completed = page_current >= int(page_last) or page_current >= (page_max, page_current + 1)[page_max is None]

    except Exception:
        logging.error('failed to load page %s', page_current, exc_info=True)
        invalidate_key(url)
        raise

    return page_content, completed


def load_pages(output_dir, page_max=None, page_start=1):
    page_counter = page_start
    results, completed = load_page(page_counter, page_max=page_max)
    while not completed:
        page_counter += 1
        page_results, completed = load_page(page_counter, page_max=page_max)
        results += page_results

    with open(os.path.sep.join([output_dir, 'ship-db.csv']), 'w') as ship_db:
        csv_writer = csv.DictWriter(ship_db, sorted(results[0].keys()))
        csv_writer.writeheader()
        csv_writer.writerows(results)


def main(args):
    if not os.path.exists(args.output_dir):
        logging.info('creating output directory "%s"', os.path.abspath(args.output_dir))
        os.makedirs(args.output_dir)

    load_pages(args.output_dir, page_max=None, page_start=1)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    logging.getLogger('requests').setLevel(logging.WARNING)
    file_handler = logging.FileHandler('download-vessels.log', mode='w')
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)

    parser = argparse.ArgumentParser(description='Importing vessels from online DB',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter
                                     )

    parser.add_argument('--output-dir', type=str, help='location of output directory', default='.')
    parser.add_argument('output_file', type=str, nargs='?', help='name of the output CSV file', default='vessels.csv')
    args = parser.parse_args()

    set_cache_path(os.path.sep.join([args.output_dir, 'urlcaching']))

    main(args)