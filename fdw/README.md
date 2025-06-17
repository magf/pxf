# PXF Foreign Data Wrapper for Greengage and PostgreSQL

This Greengage extension implements a Foreign Data Wrapper (FDW) for PXF.

PXF is a query federation engine that accesses data residing in external systems
such as Hadoop, Hive, HBase, relational databases, S3, Google Cloud Storage,
among other external systems.

### Development

## Compile

To compile the PXF foreign data wrapper, we need a Greengage 6+ installation and libcurl.

    export PATH=/usr/local/greengage-db/bin/:$PATH

    make

## Install

    make install

## Regression

    make installcheck
