from setuptools import setup, find_packages
import sys, os

version = '0.1'

setup(
    name='ckanext-enac',
    version=version,
    description="ENAC Harvester for CKAN",
    long_description="",
    classifiers=[],
    keywords='',
    author='Corentin Junod',
    author_email='corentin.junod@epfl.ch',
    url='',
    license='AGPL',
    packages=find_packages(),
    namespace_packages=['ckanext'],
    include_package_data=True,
    zip_safe=False,
    install_requires=[],
    entry_points=\
    """
    [ckan.plugins]
    enac_harvester=ckanext.enac.harvester:EnacHarvester
    """,
)
