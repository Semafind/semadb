package mrpc

import (
	"bufio"
	"io"
	"net/rpc"

	"github.com/rs/zerolog/log"
	"github.com/vmihailenco/msgpack/v5"
)

/* msgpackCodec implements the rpc.ClientCodec and rpc.ServerCodec interfaces
 * using msgpack to encode and decode the data. The implementation follows near
 * identically the net/rpc package. It is a shame the net/rpc package doesn't
 * expose an easier way to swap the codec implementation.
 *
 * Source credit: net/rpc package */
type msgpackCodec struct {
	rwc    io.ReadWriteCloser
	dec    *msgpack.Decoder
	enc    *msgpack.Encoder
	encBuf *bufio.Writer
	closed bool
}

func NewMsgpackCodec(rwc io.ReadWriteCloser) *msgpackCodec {
	buf := bufio.NewWriter(rwc)
	return &msgpackCodec{
		rwc:    rwc,
		dec:    msgpack.NewDecoder(rwc),
		enc:    msgpack.NewEncoder(buf),
		encBuf: buf,
	}
}

/* Methods for the rpc.ClientCodec interface */

func (c *msgpackCodec) WriteRequest(r *rpc.Request, body any) (err error) {
	if err = c.enc.Encode(r); err != nil {
		return
	}
	if err = c.enc.Encode(body); err != nil {
		return
	}
	return c.encBuf.Flush()
}

func (c *msgpackCodec) ReadResponseHeader(r *rpc.Response) error {
	return c.dec.Decode(r)
}

func (c *msgpackCodec) ReadResponseBody(body any) error {
	return c.dec.Decode(body)
}

/* Methods for the rpc.ServerCodec interface */

func (c *msgpackCodec) ReadRequestHeader(r *rpc.Request) error {
	return c.dec.Decode(r)
}

func (c *msgpackCodec) ReadRequestBody(body any) error {
	return c.dec.Decode(body)
}

func (c *msgpackCodec) WriteResponse(r *rpc.Response, body any) (err error) {
	if err = c.enc.Encode(r); err != nil {
		if c.encBuf.Flush() == nil {
			// Couldn't encode the header. Should not happen, so if it does,
			// shut down the connection to signal that the connection is broken.
			log.Error().Err(err).Msg("rpc: msgpack error encoding response")
			c.Close()
		}
		return
	}
	if err = c.enc.Encode(body); err != nil {
		if c.encBuf.Flush() == nil {
			// Was a msgpack problem encoding the body but the header has been written.
			// Shut down the connection to signal that the connection is broken.
			log.Error().Err(err).Msg("rpc: msgpack error encoding body")
			c.Close()
		}
		return
	}
	return c.encBuf.Flush()
}

func (c *msgpackCodec) Close() error {
	if c.closed {
		// Only call c.rwc.Close once; otherwise the semantics are undefined.
		return nil
	}
	c.closed = true
	return c.rwc.Close()
}
