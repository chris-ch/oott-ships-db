import argparse
import csv
import logging
import os
import re
from string import Template

import pandas
from bs4 import BeautifulSoup

from webscrapetools.taskpool import TaskPool
from webscrapetools.urlcaching import set_cache_path, open_url

_VESSEL_TYPES = {
    'Cargo ships': '4',
    'Tanker': '6'
}

_URL_BASE = 'https://www.vesselfinder.com'
_URL_LIST_TEMPLATE = Template(_URL_BASE + '/vessels?t=$vessel_type&page=$page_count')


def load_details(url, load_id):
    logging.info('processing url: %s', url)
    html_text = open_url(url, throttle=1)
    html = BeautifulSoup(html_text, 'html.parser')
    ais_data = html.find('div', {'id': 'ais-data'})
    params = dict()
    if ais_data is None:
        logging.warning('invalid format for page: "%s"', url)
        return load_id, params

    for param in ais_data.find_all('div', {'class': 'row param'}):
        column_name_tag = param.find('span', {'itemprop': 'name'})
        column_value_tag = param.find('span', {'itemprop': 'value'})
        if column_name_tag and column_value_tag:
            column_name = column_name_tag.text
            column_value = column_value_tag.text.strip()
            params[column_name] = column_value

        else:
            column_name_tag = param.find('span', {'class': 'name'})
            column_value_tag = param.find('span', {'class': 'value'})
            if column_name_tag and column_value_tag:
                column_name = column_name_tag.text.replace(':', '')
                column_value = column_value_tag.text.strip()
                if column_value.upper() in ('PREMIUM USERS ONLY', 'N/A'):
                    column_value = ''

                params[column_name] = column_value

    master_data = html.find('section', {'id': 'master-data'})
    for param in master_data.find_all('div', {'class': 'row param'}):
        def find_param(param_field):
            def inner(tag):
                if tag.name in ('div', 'span'):
                    for attr in tag.attrs:
                        if param_field in tag[attr]:
                            return True

                return False

            return inner

        column_name_tag = param.find(find_param('name'))
        column_value_tag = param.find(find_param('value'))
        if column_name_tag and column_value_tag:
            column_name = column_name_tag.text.replace(':', '')
            column_value = column_value_tag.text.strip()
            if column_value.upper() in ('PREMIUM USERS ONLY', 'N/A'):
                column_value = ''

            params[column_name] = column_value

    last_report_ts = None
    last_report_tag = html.find('time', {'id': 'last_report_ts'})
    if last_report_tag is not None:
        last_report_ts = last_report_tag.contents[0]

    params['last_report_ts'] = last_report_ts
    return load_id, params


def inspect(input_filename):
    with open(input_filename, 'r', encoding='utf-8') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        rows = list()
        for row in csv_reader:
            fields = {field: row[field].strip() for field in row}
            if fields['IMO'] == '':
                continue

            gross_tonnage = None
            field_gt = fields['GT'].strip()
            if field_gt.endswith(' t'):
                gross_tonnage = int(field_gt[:-2])

            row['GT'] = gross_tonnage
            length_width = re.match(r'([0-9]+)\sx\s([0-9]+)', fields['Size'])
            row['Length'], row['Width'] = None, None
            if length_width and len(length_width.groups()) == 2:
                row['Length'], row['Width'] = map(int, length_width.group(1, 2))

            del row['Size']
            rows.append(row)

        return rows


def build_vessels_df(rows):
    vessels = pandas.DataFrame(rows)
    vessel_selection = (vessels['Length'] < 400) & (vessels['GT'] > 80000)
    vessels_oil = vessels[vessel_selection & vessels['Ship type'].str.contains('Oil')]
    vessels_lng = vessels[vessel_selection & vessels['Ship type'].str.contains('LNG')]
    return vessels_oil, vessels_lng


def main():
    parser = argparse.ArgumentParser(description='Importing vessels details from online DB',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter
                                     )

    parser.add_argument('--input-dir', type=str, help='location of input directory', default='.')
    parser.add_argument('--input-file', type=str, help='name of the input CSV file', default='ship-db.csv')
    parser.add_argument('--output-dir', type=str, help='location of output directory', default='.')
    parser.add_argument('--head', type=int, help='processes only the indicated amount of lines from input file')
    parser.add_argument('--pool-size', type=int, help='number of parallel tasks', default=1)

    parser.add_argument('output_file', type=str, nargs='?', help='name of the output CSV file', default='vessels-details.csv')
    args = parser.parse_args()

    set_cache_path(os.path.sep.join([args.output_dir, 'urlcaching-details']))
    if not os.path.exists(args.output_dir):
        logging.info('creating output directory "%s"', os.path.abspath(args.output_dir))
        os.makedirs(args.output_dir)

    tasks = TaskPool(args.pool_size)
    enhanced_vessels = list()
    input_filename = os.sep.join((args.input_dir, args.input_file))
    rows = inspect(input_filename)
    vessels_oil, vessels_lng = build_vessels_df(rows)

    for count, vessel_row_data in enumerate(vessels_oil.iterrows()):
        vessel = vessel_row_data[1].to_dict()
        enhanced_vessels.append(vessel)
        if args.head is not None:
            if count >= args.head:
                break

        ship_details_url_path = vessel['ship_details_url_path']
        if ship_details_url_path.startswith('/vessels'):
            url = _URL_BASE + ship_details_url_path
            tasks.add_task(load_details, url, count)

    details = tasks.execute()

    for load_id, vessel_data in details:
        for param_name in vessel_data:
            enhanced_vessels[load_id][param_name] = vessel_data[param_name]

    with open(os.path.sep.join([args.output_dir, 'ship-db-details.csv']), 'w', encoding='utf-8') as ship_db:
        csv_writer = csv.DictWriter(ship_db, sorted(enhanced_vessels[0].keys()))
        csv_writer.writeheader()
        csv_writer.writerows(enhanced_vessels)

    logging.info('completed tasks')

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    logging.getLogger('requests').setLevel(logging.WARNING)
    file_handler = logging.FileHandler('download-vessels-details.log', mode='w')
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)
    try:
        main()
    except Exception:
        logging.exception('uncaught exception')

