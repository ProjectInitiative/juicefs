/*
 * JuiceFS, Copyright 2026 ProjectInitiative, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package readsvc

// Client for the read service. One Client = one connection; open one Client
// per concurrent worker for parallelism (handles are per-Client and the
// server processes a connection's requests sequentially).

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

// Client is a read-service connection.
type Client struct {
	conn net.Conn
	rid  uint32
	size int64
}

// Dial connects to the unix socket at sockPath.
func Dial(sockPath string) (*Client, error) {
	conn, err := net.DialTimeout("unix", sockPath, 5*time.Second)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn}, nil
}

// SetTimeout bounds each request/response exchange.
func (c *Client) SetTimeout(d time.Duration) {
	_ = c.conn.SetDeadline(time.Now().Add(d))
}

func (c *Client) call(op uint8, body []byte) ([]byte, error) {
	c.rid++
	req := make([]byte, 0, 9+len(body))
	req = append(req, op)
	req = binary.LittleEndian.AppendUint32(req, c.rid)
	req = binary.LittleEndian.AppendUint32(req, uint32(len(body)))
	req = append(req, body...)
	if _, err := c.conn.Write(req); err != nil {
		return nil, err
	}
	var hdr [9]byte
	if _, err := io.ReadFull(c.conn, hdr[:]); err != nil {
		return nil, err
	}
	rrid := binary.LittleEndian.Uint32(hdr[0:4])
	status := hdr[4]
	plen := int(binary.LittleEndian.Uint32(hdr[5:9]))
	payload := make([]byte, plen)
	if plen > 0 {
		if _, err := io.ReadFull(c.conn, payload); err != nil {
			return nil, err
		}
	}
	if rrid != c.rid {
		return nil, fmt.Errorf("response id mismatch: got %d want %d", rrid, c.rid)
	}
	if status != 0 {
		if len(payload) < 6 {
			return nil, errors.New("malformed error response")
		}
		mlen := int(binary.LittleEndian.Uint16(payload[4:6]))
		return nil, errors.New(string(payload[6 : 6+mlen]))
	}
	return payload, nil
}

// Open opens path read-only. The returned handle is bound to this Client.
func (c *Client) Open(path string) (uint64, int64, error) {
	body := make([]byte, 0, 2+len(path))
	body = binary.LittleEndian.AppendUint16(body, uint16(len(path)))
	body = append(body, path...)
	payload, err := c.call(OpOpen, body)
	if err != nil {
		return 0, 0, err
	}
	if len(payload) != 16 {
		return 0, 0, errors.New("malformed open response")
	}
	id := binary.LittleEndian.Uint64(payload[0:8])
	size := int64(binary.LittleEndian.Uint64(payload[8:16]))
	return id, size, nil
}

// Handle ties reads to one open file on this Client's connection.
type Handle struct {
	c    *Client
	id   uint64
	Size int64
}

// OpenHandle opens path and returns a Handle for ReadAt/BatchReadAt.
func (c *Client) OpenHandle(path string) (*Handle, error) {
	id, size, err := c.Open(path)
	if err != nil {
		return nil, err
	}
	return &Handle{c: c, id: id, Size: size}, nil
}

func encEntry(h *Handle, off int64, n int) []byte {
	b := make([]byte, 20)
	binary.LittleEndian.PutUint64(b[0:8], h.id)
	binary.LittleEndian.PutUint64(b[8:16], uint64(off))
	binary.LittleEndian.PutUint32(b[16:20], uint32(n))
	return b
}

// ReadAt reads len(buf) bytes at off into buf.
func (h *Handle) ReadAt(buf []byte, off int64) (int, error) {
	payload, err := h.c.call(OpReadAt, encEntry(h, off, len(buf)))
	if err != nil {
		return 0, err
	}
	if len(payload) < 4 {
		return 0, errors.New("malformed readat response")
	}
	n := int(binary.LittleEndian.Uint32(payload[0:4]))
	if n > len(buf) {
		return 0, errors.New("response larger than request")
	}
	copy(buf, payload[4:4+n])
	return n, nil
}

// BatchReadAt issues concurrent-shaped reads on one connection. The server
// executes them sequentially per connection; use one Handle per worker for
// true parallelism. Returns per-entry byte counts (negative on error).
func (h *Handle) BatchReadAt(bufs [][]byte, offs []int64) ([]int, error) {
	if len(bufs) != len(offs) {
		return nil, errors.New("bufs/offs length mismatch")
	}
	if len(bufs) > MaxBatchSize {
		return nil, fmt.Errorf("batch of %d exceeds %d", len(bufs), MaxBatchSize)
	}
	body := make([]byte, 0, 2+len(bufs)*20)
	body = binary.LittleEndian.AppendUint16(body, uint16(len(bufs)))
	for i := range bufs {
		if len(bufs[i]) > MaxReadLen {
			return nil, fmt.Errorf("entry %d exceeds read limit", i)
		}
		body = append(body, encEntry(h, offs[i], len(bufs[i]))...)
	}
	payload, err := h.c.call(OpBatch, body)
	if err != nil {
		return nil, err
	}
	if len(payload) < 2 {
		return nil, errors.New("malformed batch response")
	}
	n := int(binary.LittleEndian.Uint16(payload[0:2]))
	if n != len(bufs) {
		return nil, errors.New("batch response count mismatch")
	}
	results := make([]int, n)
	p := payload[2:]
	for i := 0; i < n; i++ {
		if len(p) < 5 {
			return nil, errors.New("truncated batch response")
		}
		st := p[0]
		l := int(binary.LittleEndian.Uint32(p[1:5]))
		p = p[5:]
		if st != 0 || l > len(bufs[i]) || len(p) < l {
			results[i] = -1
			if st != 0 {
				// skip no data
				continue
			}
		} else {
			copy(bufs[i], p[:l])
			results[i] = l
		}
		if st == 0 {
			p = p[l:]
		}
	}
	return results, nil
}

// Close releases the handle.
func (h *Handle) Close() error {
	body := make([]byte, 8)
	binary.LittleEndian.PutUint64(body, h.id)
	_, err := h.c.call(OpClose, body)
	return err
}

// Close terminates the connection (handles die with it server-side).
func (c *Client) Close() error { return c.conn.Close() }
