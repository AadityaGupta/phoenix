#!/bin/bash

cd phoenix-assembly/target

PHOENIX_ASSEMBLY=$(ls *SNAPSHOT.tar.gz)

tar -xzvf "$PHOENIX_ASSEMBLY"

mkdir -p phoenix-query-server
mkdir -p phoenix-query-server/bin
mkdir -p phoenix-query-server/lib

mv phoenix*SNAPSHOT/lib/*runnable.jar phoenix-query-server/lib
mv phoenix*SNAPSHOT/bin/* phoenix-query-server/bin

zip -r phoenix-query-server.zip phoenix-query-server/*