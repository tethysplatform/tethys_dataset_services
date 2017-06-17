import os
from setuptools import setup, find_packages
import sys

with open(os.path.join(os.path.dirname(__file__), 'README.rst')) as readme:
    README = readme.read()

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

gsconfig_lib = 'gsconfig'

if sys.version_info.major == 3:
    gsconfig_lib += '-py3'

requires = ['django',
            'future',
            'owslib',
            '{0}>=1.0.0,<1.1.0'.format(gsconfig_lib),
            'requests',
            'requests_toolbelt']

version = '1.6.1'
setup(
    name='tethys_dataset_services',
    version=version,
    packages=find_packages(),
    include_package_data=True,
    license='BSD 2-Clause License',
    description='A generic Python interface for dataset services such as CKAN and HydroShare',
    long_description=README,
    url='',
    author='Nathan Swain',
    author_email='nathan.swain@byu.net',
    classifiers=[
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',  # example license
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Topic :: Internet :: WWW/HTTP',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
    ],
    install_requires=requires,
    test_suite='tethys_dataset_services.tests'
)