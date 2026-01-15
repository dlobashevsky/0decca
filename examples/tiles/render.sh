#!/bin/bash

LEVEL=15
echo 'for zoom level 15 you need ~600G of free space and ~8-10 hours on NVMe disk'

rm -fR ./data ./db
mkdir -p ./data ./db
pushd data
wget https://planet.openstreetmap.org/pbf/planet-latest.osm.pbf -O planet.osm.pbf
popd

sudo docker run --rm -v "$PWD/data":/data -e JAVA_TOOL_OPTIONS="-Xmx32g" ghcr.io/onthegomap/planetiler:latest --osm-path=/data/planet.osm.pbf --output=/data/planet_z${LEVEL}.mbtiles --download --maxzoom=$LEVEL --render-maxzoom=$LEVEL --storage=mmap

0decca -t build.json
0decca -t tileserver.json

