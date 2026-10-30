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

// Package readsvc implements the fork's opt-in FUSE-bypassing read service
// (docs/LOW_LATENCY_CACHE_REVIEW.md §2). A local process connects over a
// Unix-domain socket and issues Open / ReadAt / BatchReadAt / Close against
// the mount's live metadata + chunk stack — no FUSE round trip, full reuse
// of JuiceFS caching and cache-group routing. Prototype scope: read-only,
// no POSIX semantics promises; see the deployment guide for the exact
// guarantees and invalidation behavior.
//
// Identity & isolation (review hardening):
//   - the caller's uid/gid/pid is taken from SO_PEERCRED and used for the
//     metadata context, so the mount's own permission model applies to every
//     open (never run as root unless the CALLER is root);
//   - non-Linux or non-unix-socket connections are closed immediately;
//   - handles are scoped per connection — an id from one connection is
//     invalid on another — and a disconnect closes that connection's
//     remaining handles;
//   - the socket should still be owned by the intended service account
//     (mode 0660 alone is not user-level authorization).
//
// Wire format (little-endian):
//
//	request:  op u8 | rid u32 | plen u32 | body[plen]
//	  0x01 Open:   klen u16, path
//	  0x02 ReadAt: handle u64, off i64, len u32
//	  0x03 Batch:  n u16, n × {handle u64, off i64, len u32}
//	  0x04 Close:  handle u64
//	response: rid u32 | status u8 | plen u32 | body
//	  status 0 (ok):
//	    Open:   handle u64, size u64
//	    ReadAt: n u32, data[n]
//	    Batch:  n u16, n × {status u8, len u32, data[len]}
//	    Close:  (empty)
//	  status != 0 (error): errno u32, mlen u16, msg[mlen]
package readsvc

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
)

// Op codes.
const (
	OpOpen   uint8 = 0x01
	OpReadAt uint8 = 0x02
	OpBatch  uint8 = 0x03
	OpClose  uint8 = 0x04
)

// Limits.
const (
	MaxReadLen = 4 << 20 // per single ReadAt / batch entry
	// MaxBatchSize bounds one BatchReadAt request entry count.
	MaxBatchSize = 256
	// MaxBatchBytes bounds the aggregate requested bytes of one batch
	// (review finding 1: capacity must follow actual lengths, and a single
	// batch must not be able to pin an unbounded response buffer).
	MaxBatchBytes = 32 << 20
	// handleIdleTTL closes handles idle past it (leak guard).
	handleIdleTTL = 10 * time.Minute
	// frameTimeout bounds one request/response exchange.
	frameTimeout = 60 * time.Second
)

// handleFile is the per-handle read surface (satisfied by *fs.File).
type handleFile interface {
	Pread(ctx meta.Context, b []byte, offset int64) (int, error)
	Close(ctx meta.Context) syscall.Errno
}

// Backend opens paths in the mount's live filesystem. The fs.FileSystem
// adapter lives in fs_backend.go.
type Backend interface {
	// Open returns a read handle and the file's size (a client-side hint).
	Open(ctx meta.Context, path string) (handleFile, int64, error)
}

type handleEntry struct {
	f        handleFile
	size     int64
	path     string
	lastUse  atomic.Int64 // unix nanos
}

// connState holds one connection's handles. Handle ids are scoped per
// connection (review finding 4): an id observed on another connection is
// invalid, and disconnect closes everything the connection left open.
type connState struct {
	mu      sync.Mutex
	handles map[uint64]*handleEntry
	next    uint64
}

func newConnState() *connState {
	return &connState{handles: make(map[uint64]*handleEntry)}
}

func (cs *connState) put(f handleFile, size int64, path string) uint64 {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.next++
	id := cs.next
	cs.handles[id] = &handleEntry{f: f, size: size, path: path, lastUse: atomic.Int64{}}
	cs.handles[id].lastUse.Store(time.Now().UnixNano())
	return id
}

func (cs *connState) get(id uint64) (*handleEntry, bool) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	e, ok := cs.handles[id]
	if ok {
		e.lastUse.Store(time.Now().UnixNano())
	}
	return e, ok
}

func (cs *connState) remove(id uint64) (*handleEntry, bool) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	e, ok := cs.handles[id]
	if ok {
		delete(cs.handles, id)
	}
	return e, ok
}

func (cs *connState) closeIdle(now time.Time) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	for id, e := range cs.handles {
		if now.UnixNano()-e.lastUse.Load() > handleIdleTTL.Nanoseconds() {
			_ = e.f.Close(openCtxFor(0, 0, 0))
			delete(cs.handles, id)
		}
	}
}

func (cs *connState) closeAll() {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	for id, e := range cs.handles {
		_ = e.f.Close(openCtxFor(0, 0, 0))
		delete(cs.handles, id)
	}
}

// Server serves the read protocol over one listener (typically a unix
// domain socket inside the mount process).
type Server struct {
	backend Backend

	mu    sync.Mutex
	conns map[*connState]struct{}

	sockPath string
	closed   atomic.Bool
}

// NewServer creates a server over backend; sockPath is removed on Close.
func NewServer(backend Backend, sockPath string) *Server {
	return &Server{
		backend:  backend,
		conns:    make(map[*connState]struct{}),
		sockPath: sockPath,
	}
}

// Serve accepts connections until ln is closed or ctx is done. Blocking.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	go s.reapIdle(ctx)
	for {
		conn, err := ln.Accept()
		if err != nil {
			if s.closed.Load() {
				return nil
			}
			select {
			case <-ctx.Done():
				return nil
			default:
			}
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			continue
		}
		go s.serveConn(ctx, conn)
	}
}

// Close removes the socket file (idempotent).
func (s *Server) Close() {
	s.closed.Store(true)
	if s.sockPath != "" {
		_ = os.Remove(s.sockPath)
	}
}

// reapIdle closes handles idle past handleIdleTTL.
func (s *Server) reapIdle(ctx context.Context) {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		now := time.Now()
		s.mu.Lock()
		conns := make([]*connState, 0, len(s.conns))
		for cs := range s.conns {
			conns = append(conns, cs)
		}
		s.mu.Unlock()
		for _, cs := range conns {
			cs.closeIdle(now)
		}
	}
}

// openCtxFor builds the request context from the CALLER's SO_PEERCRED
// identity, so the mount's permission model governs every open (review
// finding 4: never serve reads as root unless the caller is root).
func openCtxFor(uid, gid, pid uint32) meta.Context {
	return meta.NewContext(pid, uid, []uint32{gid})
}

func (s *Server) serveConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	uid, gid, pid, ok := peerCreds(conn)
	if !ok {
		// Unauthenticated transport (non-unix socket, or non-Linux build):
		// refuse rather than fall back to a privileged identity.
		return
	}
	mctx := openCtxFor(uid, gid, pid)
	cs := newConnState()
	s.mu.Lock()
	s.conns[cs] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.conns, cs)
		s.mu.Unlock()
		cs.closeAll() // disconnect closes leaked handles
	}()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		_ = conn.SetReadDeadline(time.Now().Add(frameTimeout))
		op, rid, body, err := readRequest(conn)
		if err != nil {
			return
		}
		resp := s.dispatch(cs, mctx, op, rid, body)
		_ = conn.SetWriteDeadline(time.Now().Add(frameTimeout))
		if err := writeResponse(conn, resp); err != nil {
			return
		}
	}
}

func (s *Server) dispatch(cs *connState, mctx meta.Context, op uint8, rid uint32, body []byte) response {
	switch op {
	case OpOpen:
		return s.handleOpen(cs, mctx, rid, body)
	case OpReadAt:
		return s.handleReadAt(cs, mctx, rid, body)
	case OpBatch:
		return s.handleBatch(cs, mctx, rid, body)
	case OpClose:
		return s.handleClose(cs, mctx, rid, body)
	default:
		return errResponse(rid, 0, fmt.Sprintf("unknown op 0x%x", op))
	}
}

func (s *Server) handleOpen(cs *connState, mctx meta.Context, rid uint32, body []byte) response {
	if len(body) < 2 {
		return errResponse(rid, 0, "malformed open")
	}
	n := int(binary.LittleEndian.Uint16(body[0:2]))
	if n <= 0 || n > 4096 || len(body) < 2+n {
		return errResponse(rid, 0, "malformed open path")
	}
	path := string(body[2 : 2+n])
	f, size, err := s.backend.Open(mctx, path)
	if err != nil {
		return errResponse(rid, 0, err.Error())
	}
	id := cs.put(f, size, path)
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[0:8], id)
	binary.LittleEndian.PutUint64(buf[8:16], uint64(size))
	return response{rid: rid, status: 0, body: buf}
}

func (s *Server) handleReadAt(cs *connState, mctx meta.Context, rid uint32, body []byte) response {
	if len(body) != 20 {
		return errResponse(rid, 0, "malformed readat")
	}
	id := binary.LittleEndian.Uint64(body[0:8])
	off := int64(binary.LittleEndian.Uint64(body[8:16]))
	length := int(binary.LittleEndian.Uint32(body[16:20]))
	if length > MaxReadLen {
		return errResponse(rid, 0, fmt.Sprintf("read %d exceeds limit %d", length, MaxReadLen))
	}
	e, ok := cs.get(id)
	if !ok {
		return errResponse(rid, 0, "bad handle")
	}
	buf := make([]byte, length)
	n, err := e.f.Pread(mctx, buf, off)
	if err != nil && n == 0 {
		return errResponse(rid, 0, err.Error())
	}
	resp := response{rid: rid, status: 0, body: make([]byte, 4+n)}
	binary.LittleEndian.PutUint32(resp.body[0:4], uint32(n))
	copy(resp.body[4:], buf[:n])
	return resp
}

func (s *Server) handleBatch(cs *connState, mctx meta.Context, rid uint32, body []byte) response {
	if len(body) < 2 {
		return errResponse(rid, 0, "malformed batch")
	}
	n := int(binary.LittleEndian.Uint16(body[0:2]))
	if n > MaxBatchSize || len(body) < 2+n*20 {
		return errResponse(rid, 0, "malformed batch entries")
	}
	type entry struct {
		id  uint64
		off int64
		n   int
	}
	entries := make([]entry, n)
	// Review finding 1: size the response from the ACTUAL requested lengths
	// (never 5+MaxReadLen per entry) and enforce an aggregate budget before
	// touching any handle.
	aggregate := 0
	for i := 0; i < n; i++ {
		b := body[2+i*20 : 2+(i+1)*20]
		entries[i] = entry{
			id:  binary.LittleEndian.Uint64(b[0:8]),
			off: int64(binary.LittleEndian.Uint64(b[8:16])),
			n:   int(binary.LittleEndian.Uint32(b[16:20])),
		}
		l := entries[i].n
		if l > MaxReadLen {
			l = 0 // oversized entries answer with an empty error body
		}
		if l > 0 {
			aggregate += 5 + l
		} else {
			aggregate += 5
		}
	}
	if aggregate > MaxBatchBytes {
		return errResponse(rid, 0, fmt.Sprintf("batch aggregate %d exceeds %d", aggregate, MaxBatchBytes))
	}
	out := make([]byte, 2, 2+aggregate)
	binary.LittleEndian.PutUint16(out[0:2], uint16(n))
	for _, e := range entries {
		var st byte = 0
		var data []byte
		if e.n > MaxReadLen {
			st = 1
		} else if h, ok := cs.get(e.id); ok {
			buf := make([]byte, e.n)
			got, err := h.f.Pread(mctx, buf, e.off)
			if err != nil && got == 0 {
				st = 1
			} else {
				data = buf[:got]
			}
		} else {
			st = 1
		}
		if st != 0 {
			out = append(out, 1)
			out = binary.LittleEndian.AppendUint32(out, 0)
			continue
		}
		out = append(out, 0)
		out = binary.LittleEndian.AppendUint32(out, uint32(len(data)))
		out = append(out, data...)
	}
	return response{rid: rid, status: 0, body: out}
}

func (s *Server) handleClose(cs *connState, mctx meta.Context, rid uint32, body []byte) response {
	if len(body) != 8 {
		return errResponse(rid, 0, "malformed close")
	}
	id := binary.LittleEndian.Uint64(body)
	e, ok := cs.remove(id)
	if !ok {
		return errResponse(rid, 0, "bad handle")
	}
	if eno := e.f.Close(mctx); eno != 0 {
		return errResponse(rid, 0, eno.Error())
	}
	return response{rid: rid, status: 0}
}

// ---------------------------------------------------------------- framing

type response struct {
	rid    uint32
	status uint8
	body   []byte
}

func errResponse(rid uint32, errno uint32, msg string) response {
	if len(msg) > 512 {
		msg = msg[:512]
	}
	b := make([]byte, 0, 6+len(msg))
	b = binary.LittleEndian.AppendUint32(b, errno)
	b = binary.LittleEndian.AppendUint16(b, uint16(len(msg)))
	b = append(b, msg...)
	return response{rid: rid, status: 1, body: b}
}

func readRequest(conn net.Conn) (op uint8, rid uint32, body []byte, err error) {
	// op u8 | rid u32 | plen u32
	var hdr [9]byte
	if _, err = io.ReadFull(conn, hdr[:]); err != nil {
		return
	}
	op = hdr[0]
	rid = binary.LittleEndian.Uint32(hdr[1:5])
	plen := int(binary.LittleEndian.Uint32(hdr[5:9]))
	if plen > 8192+MaxBatchSize*20 {
		err = errors.New("request too large")
		return
	}
	body = make([]byte, plen)
	if plen > 0 {
		_, err = io.ReadFull(conn, body)
	}
	return
}

func writeResponse(conn net.Conn, r response) error {
	// rid u32 | status u8 | plen u32 | body
	buf := make([]byte, 9, 9+len(r.body))
	binary.LittleEndian.PutUint32(buf[0:4], r.rid)
	buf[4] = r.status
	binary.LittleEndian.PutUint32(buf[5:9], uint32(len(r.body)))
	buf = append(buf, r.body...)
	_, err := conn.Write(buf)
	return err
}
