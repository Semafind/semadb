# Message Pack RPC

This package uses the [msgpack](https://github.com/vmihailenco/msgpack) encoder over the [net/rpc](https://pkg.go.dev/net/rpc) package. It mirrors near identically the contents of the [net/rpc](https://pkg.go.dev/net/rpc) standard library replacing only the encoding and decoding parts. We also some extra error checks.

We have chosen to use `msgpack` with RPC because it is the default encoding used for the disk operations. This way, we can be sure if items that are decoded can be re-encoded to be sent to other servers over RPC.

## Usage

The core usage is very similar to the standard `net/rpc` package. To setup a server:

```go
rpcMainServer := rpc.NewServer()
rpcMainServer.Register(myobject)
rpcServer := mrpc.NewHTTPServer(":9898", rpcMainServer)
rpcServer.ListenAndServe()
```

To dial in as a client:

```go
client, err := mrpc.DialHTTP("tcp", destination)
```