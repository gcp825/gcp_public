#!/usr/bin/python3

# Generic Launcher script for any dataflow job in the repo. 
# Assumes:
#   i) You have a package folder at the top level of the repo (e.g. skating_etl)
#   ii) Within that folder you have a .py executable with the same name as the package, and that this code contains your dataflow pipeline 
#   iii) Within that code your pipeline is instantiated in a function named 'run'.
#
# Structuring your code this way and invoking via the launcher allows for all custom packages used within your code to be identified and
#  installed upon the ephemeral VMs created to run your dataflow job. This is also dependent on the presence of the .setup.py file at the
#  top level of the repo, and the .setup.py file being referenced within your pipeline code.
#
# To run the launcher simply run the following from the local location of your launcher file:
#
#   ./launcher.py package_name    e.g. ./launcher.py skating_etl
#
# Note that launcher.py needs to have permissions set so that it is executable (none of your other dataflow/python code requires this)
#

import sys
from importlib import import_module

mod = import_module(sys.argv[1] + '.' + sys.argv[1])
run = getattr(mod,'run')

run(True)
