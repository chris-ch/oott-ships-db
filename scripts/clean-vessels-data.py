import argparse
import logging
import os

import pandas
import numpy
import csv
import re


def clean_details():
    with open('output/ship-db-details.csv', 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        rows = list()
        for row in csv_reader:
            if row['IMO'] == '':
                continue

            if row['ship_country_owner'] != row['Flag']:
                continue

            row['Built'] = (row['Built'], numpy.nan)[row['Built'] is None or row['Built'] == '']
            row['Current draught'] = (numpy.nan, row['Current draught'][:-2])[row['Current draught'].endswith(' m')]
            row['Deadweight'] = (numpy.nan, row['Deadweight'][:-2])[row['Deadweight'].endswith(' t')]
            row['Draught'] = (numpy.nan, row['Draught'][:-2])[row['Draught'].endswith(' m')]
            row['Gross Tonnage'] = (numpy.nan, row['Gross Tonnage'][:-2])[row['Gross Tonnage'].endswith(' t')]
            row['Net Tonnage'] = (numpy.nan, row['Net Tonnage'][:-2])[row['Net Tonnage'].endswith(' t')]
            row['Course'] = numpy.nan
            row['Speed'] = numpy.nan
            course_speed = re.match(r'([0-9]+)\W+([0-9\.]+)', row['Course/Speed'])
            if course_speed and len(course_speed.groups()) == 2:
                row['Course'], row['Speed'] = course_speed.group(1, 2)

            length_width = re.match(r'([0-9]+)\sx\s([0-9]+)', row['Size'])
            row['Length'], row['Width'] = numpy.nan, numpy.nan
            if length_width and len(length_width.groups()) == 2:
                row['Length'], row['Width'] = length_width.group(1, 2)

            del row['GT']
            del row['Size']
            del row['Course/Speed']
            del row['Crude (bbl)']

            rows.append(row)

        vessels = pandas.DataFrame(rows).groupby('IMO').last()
        numeric_columns = ['Course','Current draught', 'Draught', 'Width', 'Length', 'Deadweight',
                           'Gross Tonnage', 'Net Tonnage', 'Speed']
        vessels[numeric_columns] = vessels[numeric_columns].apply(pandas.to_numeric)
        vessels = vessels[vessels['Width'] < vessels['Width'].mean() + 6. * vessels['Width'].std()]
        vessels = vessels[vessels['Length'] < vessels['Length'].mean() + 6. * vessels['Width'].std()]
        vessels.to_pickle('output/vessels_df.pkl')


def inspect(input_filename):
    with open(input_filename, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        rows = list()
        for row in csv_reader:
            fields = {field: row[field].strip() for field in row}
            if fields['IMO'] == '':
                continue

            gross_tonnage = None
            if fields['GT'].endswith(' t'):
                if len(fields['GT'][:-2]) > 0:
                    gross_tonnage = int(fields['GT'][:-2])

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
    parser = argparse.ArgumentParser(description='Clean up raw exports',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter
                                     )

    parser.add_argument('--input-dir', type=str, help='location of input directory', default='.')
    parser.add_argument('--input-file', type=str, help='name of the input CSV file', default='ship-db.csv')
    args = parser.parse_args()

    input_filename = os.sep.join((args.input_dir, args.input_file))
    rows = inspect(input_filename)
    vessels_oil, vessels_lng = build_vessels_df(rows)
    print(vessels_oil.describe())
    print(vessels_lng.describe())

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    logging.getLogger('requests').setLevel(logging.WARNING)
    file_handler = logging.FileHandler('download-vessels.log', mode='w')
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)

    try:
        main()

    except:
        logging.exception('uncaught error')