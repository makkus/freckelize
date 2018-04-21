# -*- coding: utf-8 -*-

"""Top-level package for freckelize."""

from __future__ import absolute_import, division, print_function

import click

__author__ = """Markus Binsteiner"""
__email__ = 'makkus@frkl.io'
__version__ = '0.1.0'

def print_version(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    click.echo(__version__)
    ctx.exit()
