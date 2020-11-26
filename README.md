# gcp_public

---------------------------------------------------------------------------------
Overview
---------------------------------------------------------------------------------

This repository contains code used to remodel 2018 Olympic Figure Skating data from a set of denormalised csv files into a
bespoke normalised data structure.

Two different approaches for achieving this are covered in this repository:

i) An ETL approach using Google DataFlow with Python to produce a set of pipe-delimited load files outside of the database
  for upload to the physical data model (/skating_etl)
  
ii) An ELT approach using Google BigQuery & Google Cloud Composer (with Python) to load the denormalised data as-is to BigQuery
    and then transform it to the desired physical data model within the database (/skating_elt)
    
Also included within the repo are a number of bespoke common code packages of classes and functions, all written generically and reusable 
for similar ETL/ELT developments. A good proportion of that common code is used within the Figure Skating pipelines, and that which isn't
was either discarded in favour of an alternative approach at some later point or developed at the same time as a learning exercise.
Everything within these packages retains some ongoing usefulness even if not used specifically here.

This project was conceived as an 'as-close-to-real-life-as-possible' GCP learning exercise with a focus on products and techniques useful
in a data engineering context, and ultimately enabling a comparison of ETL vs ELT approaches in the GCP world.  This repo is the whole 
tangible output from this ETL vs ELT exercise... but what it doesn't adequately demonstrate are the lost hours/days of banging your head
against a brick wall trying to get code working when neither the GCP/Apache documentation nor the whole of the internet seems able to
adequately explain the specific problem(s) you are having! The contents herein will hopefully allow you to sidestep some of those issues
should you choose to use it as guideline... enjoy!
    
---------------------------------------------------------------------------------
Package Overview
---------------------------------------------------------------------------------    

**/bigquery/func** - contains generic BigQuery UDFs (i.e. not just applicable to this project)

**/gcp_tools/beam_tools.py** - generic Apache Beam/Python ETL callable functions (implemented as Classes) e.g. Sort, Transform, Lookup, Normalise

**/gcp_tools/io_tools.py** - generic python functions for up/downloading from Google Cloud Storage & Google PubSub (not actually used in skating code)

**/python_tools/scalar_functions.py** - custom python scalar functions e.g. like-for-like implementation of SQL Translate function 

**/skating_etl** - ETL implementation of the remodelling of the Figure Skating data (using Dataflow, Apache Beam, Python)

**/skating_elt** - ELT implementation of the remodelling of the Figure Skating data (using BigQuery standard SQL + Cloud Composer/Python)

**.launcher.py** - Generic Launcher module for triggering build & run of dataflow jobs within the repo (must be at top level to resolve dependencies)

**.setup.py** - Generic setup.py file used to discover dependencies for dataflow jobs within the repo (must be at top level to resolve dependencies)

---------------------------------------------------------------------------------
Licensing
---------------------------------------------------------------------------------

All code in this repository is available under the Apache 2.0 license (see accompanying LICENSE file)

Much of the code in this repository was designed to process data files provided by Buzz Feed News at the following location:

https://github.com/BuzzFeedNews/2018-02-olympic-figure-skating-analysis/tree/master/data

These files are available under the Creative Commons Attribution 4.0 International (CC BY 4.0) License, detailed here:

https://creativecommons.org/licenses/by/4.0/

