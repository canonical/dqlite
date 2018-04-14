#!/bin/sh

if [ "$1" = "" ]; then
    echo "no download directory provided"
    exit 1
fi

dir=$1
mkdir -p $dir
cd $dir

download() {
  sqlite_project=CanonicalLtd/sqlite
  sqlite_latest=https://api.github.com/repos/$sqlite_project/releases/latest
  sqlite_release=$(curl -s $sqlite_latest | grep browser_download_url | cut -d '"' -f 4|grep amd64--enable-debug)
  wget $sqlite_release -O - | tar xfz -
}

# Retry a few times since Travis sometimes fails the download
for i in $(seq 10); do
    if ! download; then
	sleep 2
	continue
    fi
    break
done
