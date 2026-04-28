#!/bin/bash

LEVEL=15
echo 'for zoom level 15 you need ~600G of free space and ~8-10 hours on NVMe disk'

rm -fR ./data ./db
mkdir -p ./data ./db
pushd data
wget https://planet.openstreetmap.org/pbf/planet-latest.osm.pbf -O planet.osm.pbf
popd

sudo docker run --rm -v "$PWD/data":/data -e JAVA_TOOL_OPTIONS="-Xmx32g" ghcr.io/onthegomap/planetiler:latest --osm-path=/data/planet.osm.pbf --output=/data/planet_z${LEVEL}.mbtiles --download --maxzoom=$LEVEL --render-maxzoom=$LEVEL --storage=mmap

echo 'render done'

sudo sqlite3 data/planet_z15.mbtiles "CREATE INDEX IF NOT EXISTS idx_shallow_id ON tiles_shallow(tile_data_id);"

time ./0decca -t build.json
rm -Rd ./data
echo 'build done, result is in DB'
# ./0decca -t tileserver.json

