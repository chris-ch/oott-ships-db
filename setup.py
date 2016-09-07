from distutils.core import setup

setup(
    name='oott-ships-db',
    version='1.0',
    packages=[''],
    package_dir={'': 'src'},
    url='',
    license='',
    author='Christophe',
    author_email='ch.alexandre@bluewin.ch',
    description='Managing Tanker DB',
    install_requires=[
        'matplotlib >= 1.5.2',
        'beautifulsoup4 >= 4.5.1',
        'pandas >= 0.18.1',
        'requests >= 2.11.1',
    ],
)
