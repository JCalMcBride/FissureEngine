from setuptools import setup, find_packages

VERSION = '1.0.7'
DESCRIPTION = 'Engine for getting fissure information in Warframe.'
LONG_DESCRIPTION = 'Engine for getting the current fissures for Warframe in a dictionary object.'


setup(
    name='fissure-engine',
    version=VERSION,
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    author='Jacob McBride',
    author_email='jake55111@gmail.com',
    packages=find_packages(),
    keywords=['warframe', 'fissures'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        'aiohttp~=3.9.3',
        'ordered-set~=4.1.0',
        'tenacity~=8.2.3'
    ],
)
