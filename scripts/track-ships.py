import argparse
import logging
import os
from string import Template

from webscrapetools.urlcaching import open_url

_VESSEL_TYPES = {
    'Cargo ships': '4',
    'Tanker': '6'
}

_URL_BASE = 'https://www.marinetraffic.com'
_URL_MAP_TEMPLATE = Template(_URL_BASE +
                             '/map/get_data_json/sw_x:$sw_x/sw_y:$sw_y/ne_x:$ne_x/ne_y:$ne_y/zoom:$zoom/station:0')


def load_map(south_west_x, south_west_y, north_east_x, north_east_y, zoom):
    south_west_x, north_east_x = ((north_east_x, south_west_x), (south_west_x, north_east_x))[south_west_x < north_east_x]
    south_west_y, north_east_y = ((north_east_y, south_west_y), (south_west_y, north_east_y))[south_west_y < north_east_y]
    url = _URL_MAP_TEMPLATE.substitute(
        {
            'sw_x': south_west_x,
            'sw_y': south_west_y,
            'ne_x': north_east_x,
            'ne_y': north_east_y,
            'zoom': zoom,
        }
    )
    #home_page = open_url('https://www.marinetraffic.com')
    #print(home_page)
    html_text = open_url(url)
    return html_text


_TRACKING_ZONES = {
    'Houston': ((-95.45, 29.83), (-93.60, 28.70)),
    'Corpus Christi': ((-96.20, 26.90), (-97.60, 28.30)),
    'New Orleans': ((-91.00, 30.50), (-88.30, 28.85)),
    'Los Angeles': ((-117.80, 33.46), (-118.60, 34.00)),
    'Test': ((-94, 18), (-89, 23)),
}


def get_tracking_zone(zone_name):
    south_west, north_east = _TRACKING_ZONES[zone_name]
    south_west_x, south_west_y = south_west
    north_east_x, north_east_y = north_east
    return south_west_x, south_west_y, north_east_x, north_east_y


def main(args):
    if not os.path.exists(args.output_dir):
        logging.info('creating output directory "%s"', os.path.abspath(args.output_dir))
        os.makedirs(args.output_dir)

    zoom = 9
    south_west_x, south_west_y, north_east_x, north_east_y = get_tracking_zone('Test')

    html = load_map(south_west_x, south_west_y, north_east_x, north_east_y, zoom)
    print(html)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    logging.getLogger('requests').setLevel(logging.WARNING)
    file_handler = logging.FileHandler('download-vessels.log', mode='w')
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)

    parser = argparse.ArgumentParser(description='Importing vessels from online map',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter
                                     )

    parser.add_argument('--output-dir', type=str, help='location of output directory', default='.')
    parser.add_argument('output_file', type=str, nargs='?', help='name of the output CSV file', default='vessels.csv')
    args = parser.parse_args()

    #set_cache_path(os.path.sep.join([args.output_dir, 'urlcaching-map']))

    #main(args)
    url = 'https://www.vesselfinder.com/vesselsonmap?bbox=-88.63406053944324%2C27.734570670977362%2C-88.45347277088857%2C27.912801695532963&zoom=12&mmsi=636016791&show_names=0&ref=14542.818928733646&pv=6'

    chrome = ('User-Agent',
              'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36')

    t = open_url('https://www.vesselfinder.com')
    print(t)
    z = open_url('https://www.vesselfinder.com/user/myfleet')
    print(z)
    x = open_url(url)
    print(x)
