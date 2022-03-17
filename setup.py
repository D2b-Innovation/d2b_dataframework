from setuptools import setup, find_packages
from pip.req import parse_requirements


install_reqs = parse_requirements("./requirements.txt")
reqs = [str(ir.req) for ir in install_reqs]

setup(
    name='d2b_data',
    version='0.1.0',
    packages=find_packages(include=['d2b_data', 'd2b_data.*']),
    install_requires=reqs

)
