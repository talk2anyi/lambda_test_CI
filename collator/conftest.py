"""Pytest config file"""
from subprocess import check_call

def pytest_sessionstart():
    """before session.main() is called to remove .pyc files"""
    print('Removing .pyc files')
    check_call("find . -name '*.pyc' -delete", shell=True)
