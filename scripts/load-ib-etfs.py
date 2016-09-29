import argparse
import logging
import os
import re
from urllib.parse import urlparse
from urllib.parse import parse_qs
import pandas
from bs4 import BeautifulSoup

from urlcaching import set_cache_path, open_url, invalidate_key

_EXCHANGES = (
("https://www.interactivebrokers.com/en/index.php?f=567&exch=chx","Chicago Stock Exchange (CHX)"),
("https://www.interactivebrokers.com/en/index.php?f=567&exch=bex","NASDAQ OMX BX (BEX)"),
("https://www.interactivebrokers.com/en/index.php?f=567&exch=arca","NYSE Arca (ARCA)"),
("https://www.interactivebrokers.com/en/index.php?f=567&exch=amex","NYSE MKT (NYSE AMEX)"),
("https://www.interactivebrokers.com/en/index.php?f=567&exch=chix_ca","Chi-X Canada"),
("https://www.interactivebrokers.com/en/index.php?f=567&exch=omega","Omega ATS (OMEGA)"),
("https://www.interactivebrokers.com/en/index.php?f=567&exch=pure","Pure Trading (PURE)"),
("https://www.interactivebrokers.com/en/index.php?f=567&exch=tse","Toronto Stock Exchange (TSE)"),
("https://www.interactivebrokers.com/en/index.php?f=567&exch=mexi","Mexican Stock Exchange (MEXI)"),
("https://www.interactivebrokers.com/en/index.php?f=567&exch=asx","Australian Stock Exchange (ASX)"),
("https://www.interactivebrokers.com/en/index.php?f=567&exch=sehk","Hong Kong Stock Exchange (SEHK)"),
("https://www.interactivebrokers.com/en/index.php?f=567&exch=nse","National Stock Exchange of India (NSE)"),
("https://www.interactivebrokers.com/en/index.php?f=567&exch=sbf","Euronext France (SBF)"),
("https://www.interactivebrokers.com/en/index.php?f=567&exch=chixde","CHI-X Europe Ltd Clearstream (CHIXDE)"),
("https://www.interactivebrokers.com/en/index.php?f=567&exch=fwb","Frankfurt Stock Exchange (FWB)"),
("https://www.interactivebrokers.com/en/index.php?f=567&exch=swb","Stuttgart Stock Exchange (SWB)"),
("https://www.interactivebrokers.com/en/index.php?f=567&exch=ibis","XETRA (IBIS)"),
("https://www.interactivebrokers.com/en/index.php?f=567&exch=chixen","CHI-X Europe Ltd Clearnet (CHIXEN)"),
("https://www.interactivebrokers.com/en/index.php?f=567&exch=aeb","Euronext NL Stocks (AEB)"),
("https://www.interactivebrokers.com/en/index.php?f=567&exch=bm","Bolsa de Madrid (BM)"),
("https://www.interactivebrokers.com/en/index.php?f=567&exch=sfb","Swedish Stock Exchange (SFB)"),
("https://www.interactivebrokers.com/en/index.php?f=567&exch=ebs","SIX Swiss Exchange (EBS)"),
("https://www.interactivebrokers.com/en/index.php?f=567&exch=chixuk","CHI-X Europe Ltd Crest (CHIXUK)"),
("https://www.interactivebrokers.com/en/index.php?f=567&exch=lse","London Stock Exchange (LSE)"),
)


def load_for_exchange(exchange_full_name, exchanges):
    instruments = list()
    html_text = open_url(exchanges[exchange_full_name], rejection_marker='To continue please enter')

    def find_stock_details_link(tag):
        is_link = tag.name == 'a'
        if is_link and 'href' in tag.attrs:
            return tag['href'].startswith("javascript:NewWindow('https://misc.interactivebrokers.com/cstools")

        return False

    try:
        html = BeautifulSoup(html_text, 'html.parser')
        stock_link_tags = html.find_all(find_stock_details_link)
        for tag in stock_link_tags:
            url = re.search(r"javascript:NewWindow\('(.*?)',", tag['href']).group(1)
            query = parse_qs(urlparse(url).query)
            if 'conid' in query.keys():
                exchange_fields = exchange_full_name.split()
                exchange_name = ' '.join(exchange_fields[:-1])
                exchange_code = exchange_fields[-1][1:-1]
                instrument_data = dict(conid=query['conid'][0], label=tag.string, exchange=exchange_name, exchange_code=exchange_code)
                instruments.append(instrument_data)

    except Exception:
        logging.error('failed to load exchange %s', exchange_full_name, exc_info=True)
        invalidate_key(exchanges[exchange_full_name])
        raise

    return instruments


def main(args):
    exchanges = dict()
    for link, exchange_name in _EXCHANGES:
        exchanges[exchange_name] = link

    instruments = list()
    for exchange_name in exchanges:
        instruments += load_for_exchange(exchange_name, exchanges)

    instruments_df = pandas.DataFrame(instruments)
    output_file = os.sep.join([args.output_dir, args.output_name + '.xlsx'])
    logging.info('saving to file %s', os.path.abspath(output_file))
    writer = pandas.ExcelWriter(output_file)
    instruments_df.to_excel(writer, 'ETFs', index=False)
    writer.save()

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    logging.getLogger('requests').setLevel(logging.WARNING)
    file_handler = logging.FileHandler('ib-etfs.log', mode='w')
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)

    parser = argparse.ArgumentParser(description='Loading ETFs data from IBrokers',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter
                                     )

    parser.add_argument('--output-dir', type=str, help='location of output directory', default='.')
    parser.add_argument('--output-name', type=str, help='name of the output file', default='etfs')
    args = parser.parse_args()

    set_cache_path(os.path.sep.join([args.output_dir, 'ib-etf-urlcaching']))

    main(args)