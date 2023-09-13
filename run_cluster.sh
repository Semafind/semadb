#!/bin/bash

echo "Starting servers..."
echo "Starting server A"
SEMADB_CONFIG=./config/serverA.yaml go run ./ &
pid[0]=$!
echo "Starting server B"
SEMADB_CONFIG=./config/serverB.yaml go run ./ &
pid[1]=$!
echo "Starting server C"
SEMADB_CONFIG=./config/serverC.yaml go run ./ &
pid[2]=$!
jobs
trap "kill ${pid[0]} ${pid[1]} ${pid[2]}; exit 1" INT
wait