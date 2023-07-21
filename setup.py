from setuptools import setup, find_packages

VERSION = '0.1.22'
DESCRIPTION = 'Engine for getting fissure information in Warframe.'
LONG_DESCRIPTION = 'Engine for getting the current fissures for Warframe in a dictionary object.'

setup(
        name="fissure_engine", 
        version=VERSION,
        author="Jacob McBride",
        author_email="jake55111@gmail.com",
        description=DESCRIPTION,
        long_description=LONG_DESCRIPTION,
        packages=find_packages(),
        install_requires=[],
        keywords=['warframe','fissures'],
        classifiers= [
            "Programming Language :: Python :: 3",
            "Operating System :: OS Independent",
        ]
)