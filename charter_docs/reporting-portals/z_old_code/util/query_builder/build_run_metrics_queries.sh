#!/bin/bash

# query builder input parameters specifices runs for the query builder
qbipf=bin/qb_input_parameters.csv
# Alias for domain_mappings lookup these are joined to the sourceNames in the qbipf
dmf=bin/domain_mapping.csv

# iterate through input query builder input parameters output queries for each
python bin/buildQuery.py $qbipf $dmf
