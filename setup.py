from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="sqless",
    version="0.1.1",
    author="pro1515151515",
    author_email="pro1515151515@qq.com",
    description="An async HTTP server for SQLite, FileStorage and WebPage.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/pro1515151515/sqless",
    packages=find_packages(),
    include_package_data=True,
    package_data={
        "sqless": [
            "openapi.yaml",
            "docs.html"
        ],
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
    ],
    python_requires=">=3.7",
    install_requires=[
        "aiosqlite>=0.17.0",
        "aiohttp>=3.8.0",
        "orjson>=3.6.0",
        "aiofiles>=0.8.0",
    ],
    entry_points={
        "console_scripts": [
            "sqless=sqless.server:main",
        ],
    },
)