import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="d2b_framework",
    version="0.0.1",
    author="Paulo Kemen Plaza",
    author_email="paulo.plaza.ibarra@gmail.com",
    description="Company frameword",
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
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.6",
)
