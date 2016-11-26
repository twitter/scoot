#!/usr/bin/env python

from setuptools import setup

setup(
  name = 'Scoot',
  version = '0.5',
  description = 'Scoot library for connecting to a local Scoot Daemon Server',
  url = 'https://github.com/scootdev/scoot',
  packages = ['scoot'],
  install_requires = ['grpcio']
)
