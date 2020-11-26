#!/usr/bin/python3

# Generic Launcher script for any dataflow job in the repo. 
# Assumes the dataflow pipeline is instantiated in a function named 'run'.

import sys
from importlib import import_module

mod = import_module(sys.argv[1] + '.' + sys.argv[1])
run = getattr(mod,'run')

run(True)
