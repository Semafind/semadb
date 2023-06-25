#!/bin/bash

export SEMADB_SERVERS=localhost:11001,localhost:11002,localhost:11003
echo $SEMADB_SERVERS
echo "Starting servers..."
echo "Starting server 1"
SEMADB_RPC_HOST=localhost SEMADB_RPC_PORT=11001 go run ./server &
echo "Starting server 2"
SEMADB_RPC_HOST=localhost SEMADB_RPC_PORT=11002 go run ./server &
echo "Starting server 3"
SEMADB_RPC_HOST=localhost SEMADB_RPC_PORT=11003 go run ./server &