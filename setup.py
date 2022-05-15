from setuptools import setup

setup(
    name='alephvault-http-mongodb-storage',
    version='0.0.3',
    packages=['http_storage', 'http_storage.core', 'http_storage.types', 'http_storage.engine'],
    url='https://github.com/AlephVault/http-mongodb-storage',
    license='MIT',
    author='luismasuelli',
    author_email='luismasuelli@hotmail.com',
    description='A lightweight server to work as a simple storage for games, done with MongoDB and Flask',
    install_requires=open("requirements.txt").readlines()
)
