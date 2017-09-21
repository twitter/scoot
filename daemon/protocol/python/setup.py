#!/usr/bin/env python

from setuptools import setup
from SystemEvents.Folder_Actions_Suite import scripts

setup(
  name = 'Scoot',
  version = '0.5',
  description = 'Scoot library for connecting to a local Scoot Daemon Server',
  url = 'https://github.com/twitter/scoot',
  packages = ['scoot'],
  install_requires = ['grpcio', 'docopt'],
  scripts = ['scoot/scoot.py']
)
