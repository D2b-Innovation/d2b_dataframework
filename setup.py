from setuptools import setup, find_packages

setup(
    name='d2b_data',
    version='0.2.2',
    packages=find_packages(include=['d2b_data', 'd2b_data.*']),
    install_requires=[
        'httplib2',
        'pandas',
        'oauth2client',
        'google-api-python-client',
        'google-analytics-data',
        'google-cloud',
        'google-cloud-core',
        'google-cloud-bigquery',
        'facebook_business',
        'future',
        'twitter-ads',
        'future',

    ]
)
