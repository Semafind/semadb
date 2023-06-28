#!/bin/bash

export SEMADB_DEBUG=true
export SEMADB_SERVERS=localhost:11001,localhost:11002,localhost:11003
echo $SEMADB_SERVERS
echo "Starting servers..."
echo "Starting server 1"
SEMADB_RPC_HOST=localhost SEMADB_RPC_PORT=11001 go run ./server &
pid[0]=$!
echo "Starting server 2"
SEMADB_RPC_HOST=localhost SEMADB_RPC_PORT=11002 go run ./server &
pid[1]=$!
echo "Starting server 3"
SEMADB_RPC_HOST=localhost SEMADB_RPC_PORT=11003 go run ./server &
pid[2]=$!
jobs
trap "kill ${pid[0]} ${pid[1]} ${pid[2]}; exit 1" INT
wait