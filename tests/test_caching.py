import random

from urlcaching import set_cache_path, read_cached


def open_test_random(key):

    def inner_open_test_random(inner_key):
        return 'content for key %s: %s' % (inner_key, random.randint(1, 100000))

    content = read_cached(inner_open_test_random, key)
    return content


if __name__ == '__main__':
    set_cache_path('output/tests', max_node_files=4, rebalancing_limit=10)
    for count in range(1000):
        content = open_test_random(count)

    pass
