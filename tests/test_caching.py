import logging
import random

from taskpool import TaskPool
from urlcaching import set_cache_path, read_cached, delete_cache


def open_test_random(key):

    def inner_open_test_random(inner_key):
        return 'content for key %s: %s' % (inner_key, random.randint(1, 100000))

    content = read_cached(inner_open_test_random, key)
    return content

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    logging.getLogger('requests').setLevel(logging.WARNING)
    file_handler = logging.FileHandler('download-vessels-details.log', mode='w')
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)

    set_cache_path('output/tests', max_node_files=100, rebalancing_limit=1000)
    delete_cache()

    tasks = TaskPool()

    for count in range(10000):
        tasks.add_task(open_test_random, count)

    results = tasks.execute()
    logging.info('results: %s', results)
    delete_cache()
