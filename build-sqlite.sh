#!/bin/sh -ex

if [ -f .sqlite/.libs/libsqlite3.so ]; then
    exit 0
fi

if ! [ -d .sqlite/.git ]; then
  # Download the sqlite fork with replication support
  git clone --depth 10 --single-branch --branch replication-support https://github.com/dqlite/sqlite.git .sqlite
fi

cd .sqlite

# Make the fossil VCS happy, see http://repo.or.cz/sqlite.git.
git rev-parse --git-dir >/dev/null
git log -1 --format=format:%ci%n | sed -e 's/ [-+].*$//;s/ /T/;s/^/D /' > manifest
echo $(git log -1 --format=format:%H) > manifest.uuid

./configure --enable-debug
make
