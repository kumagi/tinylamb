#!/usr/bin/env python3
"""TPC-H SF1 embedded comparison using the shared sequential correctness/time gate.

Requires an existing DBGEN dataset and matching native fixture.
"""
from tpcds import main

if __name__ == '__main__':
    main('tpch')
