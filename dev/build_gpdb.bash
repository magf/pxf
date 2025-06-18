#!/bin/bash

pushd ~/workspace/gpdb
make clean
./configure \
  --enable-debug \
  --with-perl \
  --with-python \
  --with-libxml \
  --disable-orca \
  --prefix=/usr/local/greengage-db-devel
make -j8
popd