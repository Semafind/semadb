package rpcapi

import "github.com/rs/zerolog/log"

// ---------------------------

type WriteKVRequest struct {
	RequestArgs
	Key       []byte
	Value     []byte
	Timestamp int64
}

type WriteKVResponse struct {
}

func (api *RPCAPI) WriteKV(args *WriteKVRequest, reply *WriteKVResponse) error {
	log.Debug().Str("key", string(args.Key)).Str("host", api.MyHostname).Msg("RPCAPI.WriteKV")
	if args.Dest != api.MyHostname {
		return api.internalRoute("RPCAPI.WriteKV", args, reply)
	}
	return api.kvstore.Insert(args.Key, args.Value, args.Timestamp)
}
