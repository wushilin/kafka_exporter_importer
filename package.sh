#!/bin/sh

BASE_NAME=kafka_exporter_importer
BINARIES="exporter importer"
if [ "X$1" = "X" ]; then
   echo "Version required"
   exit
fi
echo "Pulling GIT"
git pull
echo "Building"
sh build.sh
VERSION=$1
echo Building version $VERSION
DIR=target/x86_64-unknown-linux-musl/release
cd $DIR
TARGET=/tmp/$BASE_NAME-linux-$VERSION.tar.gz
rm -f $TARGET
echo "Creating file $TARGET"
echo "Adding binaries $BINARIES"
tar zcvf $TARGET $BINARIES
cd - 2>&1 >>/dev/null
