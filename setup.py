from setuptools import setup, find_packages

setup(
    name='d2b_data',
    version='0.1.0',
    packages=find_packages(include=['d2b_data', 'd2b_data.*']),
    install_requires=[
        'httplib2',
        'pandas',
        'google-api-python-client',
        'oauth2client',
        'google-cloud',
        'google-cloud-core',
        'google-cloud-bigquery',
        'facebook_business',
        'future',

    ]
)
