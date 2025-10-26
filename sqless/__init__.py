"""
sqless - An async HTTP server for SQLite, FileStorage and WebPage.
"""

__version__ = "0.1.0"
__author__ = "pro1515151515"
__email__ = "pro1515151515@qq.com"

from .database import DB
from .server import run_server

def hello():
    """A simple function to test the package."""
    return "Hello from sqless!"