import os
from setuptools import setup, find_packages

with open(os.path.join(os.path.dirname(__file__), 'README.md')) as readme:
    README = readme.read()

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

requirements_file = 'requirements.txt'

with open(requirements_file, 'r') as r:
    requires = r.read()

version = '2.0.1'
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
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Topic :: Internet :: WWW/HTTP',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
    ],
    install_requires=requires,
    test_suite='tethys_dataset_services.tests'
)
