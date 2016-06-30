#!/bin/bash

# create database tables and load column files
psql -h localhost -U postgres -v v1=$1 -f ../sqlSync/scripts/createSchema.sql
psql -h localhost -U postgres -v v1=$1 -f ../sqlSync/scripts/loadColumnFiles.sql

# create row view
psql -h localhost -U postgres -f ../sqlSync/scripts/createSqlSyncRowView.sql

# update all bibcodes in metrics database
#python ../metrics/metrics.py metricsCompute
