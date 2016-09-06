import pandas
import numpy
import csv
import re

if __name__ == '__main__':
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
        print(vessels.head(20))
