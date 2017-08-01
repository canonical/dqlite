#!/bin/sh -ex

TAG=replication-support-3.19.2
URL=https://github.com/dqlite/sqlite/releases/download/$TAG/release--enable-debug.tar.gz

# Download the sqlite fork with replication support
mkdir -p .sqlite
cd .sqlite
rm -f *
wget $URL

tar xfz release--enable-debug.tar.gz
