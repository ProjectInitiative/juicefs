package gcache

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"time"
)

// sendWrite sends one request frame with payload (Push) or without (Drop),
// reads the response, and returns an error on MsgError / transport failure.
func (c *RingClient) sendWrite(ctx context.Context, conn net.Conn, msgType uint8, key string, payload []byte) error {
	_ = conn.SetDeadline(time.Now().Add(c.timeout))
	hdr := make([]byte, headerLen)
	hdr[0], hdr[1], hdr[2] = magic0, magic1, protoVer
	hdr[3] = msgType
	hdr[4] = WireRaw // pushed bytes are volume-format
	binary.LittleEndian.PutUint32(hdr[5:9], 0)
	binary.LittleEndian.PutUint16(hdr[9:11], uint16(len(key)))
	if payload != nil {
		binary.LittleEndian.PutUint64(hdr[12:20], uint64(len(payload)))
	}
	if err := ctxSend(ctx, conn, hdr); err != nil {
		return err
	}
	if err := ctxSend(ctx, conn, []byte(key)); err != nil {
		return err
	}
	if payload != nil {
		if err := ctxSend(ctx, conn, payload); err != nil {
			return err
		}
	}
	// response: MsgBlockResp plen=0 on success, MsgError otherwise
	_, _, plen, isErr, rerr := c.readRespHeader(ctx, conn)
	if rerr != nil {
		return rerr
	}
	if isErr {
		return fmt.Errorf("peer rejected %s: %v", "write", rerr)
	}
	if plen != 0 {
		return fmt.Errorf("unexpected non-empty push/drop response (plen=%d)", plen)
	}
	return nil
}

// Push sends one block to its ring owner (--fill-group-cache). Fire-and-
// forget semantics: a push failure never evicts a peer or blocks the
// caller's upload path; it is logged and counted.
func (c *RingClient) Push(ctx context.Context, group, key string, data []byte, members []Member) {
	if len(members) == 0 {
		return
	}
	owners := Owners(key, members, 1)
	if len(owners) == 0 || owners[0].UUID == c.selfUUID {
		return // self-owned: nothing to push
	}
	owner := owners[0]
	peer := owner.Addr
	start := time.Now()
	conn, _, derr := c.dialFailover(ctx, owner)
	var err error
	if derr == nil {
		err = c.sendWrite(ctx, conn, MsgBlockPush, key, data)
		_ = conn.Close()
	} else {
		err = derr
	}
	used := time.Since(start)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		logger.Debugf("gcache push %s to %s: %v", key, peer, err)
		if metricErrors != nil {
			metricErrors.Inc()
		}
		return
	}
	observeFetch(peer, int64(len(data)), nil, used.Seconds())
}

// Drop broadcasts a delete for key to all live non-self members. All errors
// are best-effort: stale copies are harmless and evicted naturally.
func (c *RingClient) Drop(ctx context.Context, group, key string, members []Member) {
	for i := range members {
		if members[i].UUID == c.selfUUID {
			continue
		}
		m := members[i]
		peer := m.Addr
		start := time.Now()
		conn, _, derr := c.dialFailover(ctx, m)
		var err error
		if derr == nil {
			err = c.sendWrite(ctx, conn, MsgBlockDrop, key, nil)
			_ = conn.Close()
		} else {
			err = derr
		}
		if err != nil {
			logger.Debugf("gcache drop %s to %s: %v", key, peer, err)
			continue
		}
		observeFetch(peer, 0, nil, time.Since(start).Seconds())
	}
}

// dropTimeout bounds individual Drop attempts (delete-broadcast is
// best-effort and must not queue behind slow peers).
const dropTimeout = 5 * time.Second
