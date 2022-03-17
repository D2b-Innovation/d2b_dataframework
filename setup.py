import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="d2b_framework",
    version="0.0.1",
    author="Paulo Kemen Plaza",
    author_email="paulo.plaza.ibarra@gmail.com",
    description="Company framework",
    package_data={'pykemen': ['d2b_dataframework/*.py', '*']},
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://gitlab.com/analytics67/d2b_dataframework.git",
    project_urls={
        "Bug Tracker": "https://gitlab.com/analytics67/d2b_dataframework.git",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_dir={"d2b_dataframework": "d2b_dataframework"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.6",
    install_requires=[
        'httplib2',
        'pandas',
        'google-api-python-client',
        'oauth2client',
        'google-cloud',
        'google-cloud-core',
        'google-cloud-bigquery',
        'future'
    ]
)
