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

package gcache

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
)

// serverFillKey marks a Get issued by the peer server's own fill path
// (serveBlock → FillFromStorage → load → Get). The decorator MUST pass
// these straight to the inner storage: otherwise the owner's fill itself
// triggers a peer fetch, and two nodes can deadlock fetching the same
// block from each other.
type serverFillKey struct{}

func withServerFill(ctx context.Context) context.Context {
	return context.WithValue(ctx, serverFillKey{}, true)
}

func isServerFill(ctx context.Context) bool {
	v, _ := ctx.Value(serverFillKey{}).(bool)
	return v
}

// ServeConn handles one peer connection (CONTRACT.md §6 frames). It loops
// over MsgBlockReq frames until EOF or ctx cancellation, serving each from
// src: LoadCached first; on os.ErrNotExist, FillFromStorage. Payloads are
// volume-format bytes: WireRaw when the volume is uncompressed, otherwise
// re-compressed from the decompressed cache bytes via CompressPayload.
// Errors are answered with a MsgError frame (and do NOT end the conn, so a
// client-side retry can proceed on the same connection).
func ServeConn(ctx context.Context, conn net.Conn, src ServerSource, timeout time.Duration) {
	if timeout <= 0 {
		timeout = DefaultRemoteTimeout
	}
	defer conn.Close()
	// Hard-close the conn when ctx is done, unblocking any pending read.
	stop := context.AfterFunc(ctx, func() { _ = conn.SetDeadline(pastDeadline()) })
	defer stop()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		_ = conn.SetReadDeadline(time.Now().Add(timeout))
		hdr := make([]byte, headerLen)
		if _, err := io.ReadFull(conn, hdr); err != nil {
			return // EOF / timeout: normal connection end
		}
		if hdr[0] != magic0 || hdr[1] != magic1 || hdr[2] != protoVer {
			_ = sendError(ctx, conn, errors.New("bad request header"), timeout)
			return
		}
		switch hdr[3] {
		case MsgBlockReq, MsgRangeReq, MsgBlockPush, MsgBlockDrop:
		default:
			_ = sendError(ctx, conn, fmt.Errorf("bad request type 0x%x", hdr[3]), timeout)
			return
		}
		klen := int(binary.LittleEndian.Uint16(hdr[9:11]))
		if klen == 0 || klen > 4096 {
			_ = sendError(ctx, conn, errors.New("invalid key length"), timeout)
			return
		}
		key := make([]byte, klen)
		if _, err := io.ReadFull(conn, key); err != nil {
			return
		}
		switch hdr[3] {
		case MsgBlockReq:
			servData(ctx, conn, src, string(key), timeout)
		case MsgRangeReq:
			servRange(ctx, conn, src, string(key), timeout)
		case MsgBlockPush:
			servPush(ctx, conn, src, string(key), hdr, timeout)
		case MsgBlockDrop:
			if err := src.DropCached(string(key)); err != nil {
				_ = sendError(ctx, conn, err, timeout)
				continue
			}
			_ = writeResp(ctx, conn, WireRaw, bytes.NewReader(nil), timeout)
		default:
			_ = sendError(ctx, conn, fmt.Errorf("bad request type 0x%x", hdr[3]), timeout)
		}
	}
}

// maxBlockPayload bounds ReadAll as a safety net against a corrupt source
// reporting a bogus size (blocks are ≤ 16MiB even at the largest setting).
const maxBlockPayload = 64 << 20

// servPush stores a pushed block (--fill-group-cache) and replies.
func servPush(ctx context.Context, conn net.Conn, src ServerSource, key string, hdr []byte, timeout time.Duration) {
	plen := binary.LittleEndian.Uint64(hdr[12:20])
	if plen > maxBlockPayload {
		_ = sendError(ctx, conn, fmt.Errorf("payload too large: %d", plen), timeout)
		return
	}
	// Chunked read with a refreshed read deadline per chunk: a slow client
	// must not blow a single absolute deadline mid-payload.
	data := make([]byte, plen)
	for off := 0; off < len(data); {
		end := off + writeChunk
		if end > len(data) {
			end = len(data)
		}
		_ = conn.SetReadDeadline(time.Now().Add(timeout))
		if _, err := io.ReadFull(conn, data[off:end]); err != nil {
			_ = sendError(ctx, conn, fmt.Errorf("read payload: %w", err), timeout)
			return
		}
		off = end
	}
	if err := src.StorePushed(ctx, key, data); err != nil {
		_ = sendError(ctx, conn, err, timeout)
		return
	}
	_ = writeResp(ctx, conn, WireRaw, bytes.NewReader(nil), timeout)
}

// serveBlock resolves one key and writes the response frame. No absolute
// deadline is set here: cache fill, compression and payload writes each get
// fresh per-chunk deadlines inside writeResp. A single deadline spanning the
// whole serve would abort mid-payload whenever the fill (object-storage
// download on the owner) or a slow consumer exceeds the timeout — surfacing
// on the client as a truncated stream ("unexpected EOF").
func servData(ctx context.Context, conn net.Conn, src ServerSource, key string, timeout time.Duration) {
	if err := serveBlock(ctx, conn, src, key, timeout); err != nil {
		logger.Debugf("gcache serve %s: %v", key, err)
	}
}

// keyOrigSize parses the trailing "_<size>" block size from a plain-mode key
// (CONTRACT.md §1). Returns 0 when absent or unparseable (hash-prefix keys
// embed no size). Used to validate ranged requests defensively; the authoritative
// clamp is the source's own read.
func keyOrigSize(key string) int64 {
	i := strings.LastIndexByte(key, '_')
	if i < 0 {
		return 0
	}
	n, err := strconv.ParseInt(key[i+1:], 10, 64)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// servRange answers MsgRangeReq(key, off, len) with exactly the requested
// bytes of the block's raw (decompressed) representation. The owner serves
// positionally from its cache (LoadCachedRange); on owner miss it fills its
// designated copy from object storage first (the explicit cache-fill policy:
// 4 MiB object-storage→owner once, then 4 KiB owner→client per read).
func servRange(ctx context.Context, conn net.Conn, src ServerSource, key string, timeout time.Duration) {
	if err := serveRange(ctx, conn, src, key, timeout); err != nil {
		logger.Debugf("gcache range-serve %s: %v", key, err)
	}
}

func serveRange(ctx context.Context, conn net.Conn, src ServerSource, key string, timeout time.Duration) error {
	var pair [16]byte
	if err := ctxRead(ctx, conn, pair[:]); err != nil {
		return err
	}
	off := int64(binary.LittleEndian.Uint64(pair[0:8]))
	n := int64(binary.LittleEndian.Uint64(pair[8:16]))
	if off < 0 || n <= 0 || n > maxRangePayload {
		return sendError(ctx, conn, fmt.Errorf("invalid range off=%d len=%d", off, n), timeout)
	}
	if size := keyOrigSize(key); size > 0 {
		if off >= size {
			return sendError(ctx, conn, fmt.Errorf("range past end of block: off=%d size=%d", off, size), timeout)
		}
		if off+n > size {
			n = size - off
		}
	}
	rs, ok := src.(RangeSource)
	if !ok {
		return sendError(ctx, conn, errors.New("range reads not supported by this peer's source"), timeout)
	}
	r, err := rs.LoadCachedRange(key, off, n)
	if errors.Is(err, os.ErrNotExist) {
		// Owner miss: fill the designated copy from object storage (whole
		// block, persisted), then serve the range from it.
		if _, ferr := src.FillFromStorage(withServerFill(ctx), key); ferr != nil {
			return sendError(ctx, conn, ferr, timeout)
		}
		r, err = rs.LoadCachedRange(key, off, n)
	}
	if err != nil {
		return sendError(ctx, conn, err, timeout)
	}
	defer r.Close()
	// Read the window (bounded by maxRangePayload). A short read (block ends
	// inside the window) is served as-is; the client treats plen != requested
	// as a protocol error and falls back to the full-block path.
	data := make([]byte, n)
	read, rerr := io.ReadFull(r, data)
	if rerr != nil && !errors.Is(rerr, io.ErrUnexpectedEOF) && !errors.Is(rerr, io.EOF) {
		return sendError(ctx, conn, fmt.Errorf("read cached range: %w", rerr), timeout)
	}
	observeRangeServe(int(n))
	if err := writeResp(ctx, conn, WireRaw, bytes.NewReader(data[:read]), timeout); err != nil {
		return fmt.Errorf("write range response: %w", err)
	}
	return nil
}

func serveBlock(ctx context.Context, conn net.Conn, src ServerSource, key string, timeout time.Duration) error {
	r, err := src.LoadCached(key)
	if errors.Is(err, os.ErrNotExist) {
		r, err = src.FillFromStorage(withServerFill(ctx), key)
	}
	if err != nil {
		return sendError(ctx, conn, err, timeout)
	}
	defer r.Close()

	data, err := io.ReadAll(io.LimitReader(r, maxBlockPayload+1))
	if err != nil {
		return sendError(ctx, conn, fmt.Errorf("read cached block: %w", err), timeout)
	}
	if len(data) > maxBlockPayload {
		return sendError(ctx, conn, errors.New("block exceeds payload limit"), timeout)
	}

	algo := WireRaw
	payload := bytes.NewReader(data)
	if src.CompressAlgo() != "none" && len(data) > 0 {
		dst := make([]byte, src.CompressBound(len(data)))
		n, cerr := src.CompressPayload(dst, data)
		if cerr != nil {
			// Serving raw bytes would corrupt the volume format (the
			// client's load() would try to decompress them) — fail the
			// request instead so the caller falls back to object storage.
			return sendError(ctx, conn, fmt.Errorf("compress block: %w", cerr), timeout)
		}
		algo = WireCompressed
		payload = bytes.NewReader(dst[:n])
	}

	if err := writeResp(ctx, conn, algo, payload, timeout); err != nil {
		return fmt.Errorf("write response: %w", err)
	}
	return nil
}

// writeChunk bounds each payload read/write so deadlines are refreshed per
// chunk (idle-based liveness) instead of one absolute deadline for the whole
// transfer. Writing via conn.Write per chunk also keeps the zero-copy
// ReadFrom path out of reach (the old writerOnly dance).
const writeChunk = 512 << 10

// writeResp writes a MsgBlockResp header followed by the payload stream,
// with a fresh write deadline per chunk.
func writeResp(ctx context.Context, conn net.Conn, algo uint8, payload *bytes.Reader, timeout time.Duration) error {
	hdr := make([]byte, headerLen)
	hdr[0], hdr[1], hdr[2] = magic0, magic1, protoVer
	hdr[3] = MsgBlockResp
	hdr[4] = algo
	binary.LittleEndian.PutUint32(hdr[5:9], 0) // xid echo (v1: 0)
	binary.LittleEndian.PutUint16(hdr[9:11], 0)
	binary.LittleEndian.PutUint64(hdr[12:20], uint64(payload.Len()))
	if err := ctxSend(ctx, conn, hdr); err != nil {
		return err
	}
	buf := make([]byte, writeChunk)
	for payload.Len() > 0 {
		_ = conn.SetWriteDeadline(time.Now().Add(timeout))
		n, rerr := payload.Read(buf)
		if n > 0 {
			if _, werr := conn.Write(buf[:n]); werr != nil {
				return werr
			}
		}
		if rerr != nil {
			return rerr
		}
	}
	return nil
}

// sendError writes a MsgError frame: plen=^uint64(0), then klen+msg.
func sendError(ctx context.Context, conn net.Conn, cause error, timeout time.Duration) error {
	_ = conn.SetWriteDeadline(time.Now().Add(timeout))
	msg := cause.Error()
	if len(msg) > 4096 {
		msg = msg[:4096]
	}
	hdr := make([]byte, headerLen)
	hdr[0], hdr[1], hdr[2] = magic0, magic1, protoVer
	hdr[3] = MsgError
	binary.LittleEndian.PutUint32(hdr[5:9], 0)
	binary.LittleEndian.PutUint16(hdr[9:11], uint16(len(msg)))
	binary.LittleEndian.PutUint64(hdr[12:20], ^uint64(0))
	if err := ctxSend(ctx, conn, hdr); err != nil {
		return err
	}
	return ctxSend(ctx, conn, []byte(msg))
}

// passthroughStorage delegates every ObjectStorage method to inner.
// ringStorage embeds it and overrides Get only.
type passthroughStorage struct{ inner object.ObjectStorage }

func (p *passthroughStorage) String() string                   { return p.inner.String() }
func (p *passthroughStorage) Limits() object.Limits            { return p.inner.Limits() }
func (p *passthroughStorage) Create(ctx context.Context) error { return p.inner.Create(ctx) }
func (p *passthroughStorage) Get(ctx context.Context, key string, off, limit int64, getters ...object.AttrGetter) (io.ReadCloser, error) {
	return p.inner.Get(ctx, key, off, limit, getters...)
}
func (p *passthroughStorage) Put(ctx context.Context, key string, in io.Reader, getters ...object.AttrGetter) error {
	return p.inner.Put(ctx, key, in, getters...)
}
func (p *passthroughStorage) Copy(ctx context.Context, dst, src string) error {
	return p.inner.Copy(ctx, dst, src)
}
func (p *passthroughStorage) Delete(ctx context.Context, key string, getters ...object.AttrGetter) error {
	return p.inner.Delete(ctx, key, getters...)
}
func (p *passthroughStorage) Head(ctx context.Context, key string) (object.Object, error) {
	return p.inner.Head(ctx, key)
}
func (p *passthroughStorage) List(ctx context.Context, prefix, startAfter, token, delimiter string, limit int64, followLink bool) ([]object.Object, bool, string, error) {
	return p.inner.List(ctx, prefix, startAfter, token, delimiter, limit, followLink)
}
func (p *passthroughStorage) ListAll(ctx context.Context, prefix, marker string, followLink bool) (<-chan object.Object, error) {
	return p.inner.ListAll(ctx, prefix, marker, followLink)
}
func (p *passthroughStorage) CreateMultipartUpload(ctx context.Context, key string) (*object.MultipartUpload, error) {
	return p.inner.CreateMultipartUpload(ctx, key)
}
func (p *passthroughStorage) UploadPart(ctx context.Context, key, uploadID string, num int, body []byte) (*object.Part, error) {
	return p.inner.UploadPart(ctx, key, uploadID, num, body)
}
func (p *passthroughStorage) UploadPartCopy(ctx context.Context, key, uploadID string, num int, srcKey string, off, size int64) (*object.Part, error) {
	return p.inner.UploadPartCopy(ctx, key, uploadID, num, srcKey, off, size)
}
func (p *passthroughStorage) AbortUpload(ctx context.Context, key, uploadID string) {
	p.inner.AbortUpload(ctx, key, uploadID)
}
func (p *passthroughStorage) CompleteUpload(ctx context.Context, key, uploadID string, parts []*object.Part) error {
	return p.inner.CompleteUpload(ctx, key, uploadID, parts)
}
func (p *passthroughStorage) ListUploads(ctx context.Context, marker string) ([]*object.PendingPart, string, error) {
	return p.inner.ListUploads(ctx, marker)
}
func (p *passthroughStorage) Restore(ctx context.Context, key string, days int32) error {
	return p.inner.Restore(ctx, key, days)
}

// ringStorage is the client-side decorator: it intercepts full-object Gets
// (off==0 && (limit==-1 || limit==blockSize), the shapes cachedStore issues
// for whole-block fetches — CONTRACT §2) and serves ring-owned blocks from
// the owning peer; everything else
// falls through to the wrapped ObjectStorage untouched.
type ringStorage struct {
	passthroughStorage
	rc           *RingClient
	group        string // registry group whose members() closure is wired
	members      func(group string) []Member
	selfUUID     string
	fillOnUpload bool  // --fill-group-cache: push uploaded blocks to owners
	blockSize    int64 // chunk block size; 0 = only limit==-1 counts as full-block
}

// SetFillOnUpload enables --fill-group-cache push-on-upload semantics on a
// ringStorage returned by NewRingStorage (no-op for any other storage).
func SetFillOnStorage(os object.ObjectStorage, v bool) {
	if rs, ok := os.(*ringStorage); ok {
		rs.fillOnUpload = v
	}
}

// SetBlockSizeOnStorage teaches the decorator the chunk block size so that
// full-block Gets issued with an explicit length (off==0, limit==blockSize)
// engage peer placement, not just limit==-1. No-op for other storages.
func SetBlockSizeOnStorage(os object.ObjectStorage, n int64) {
	if rs, ok := os.(*ringStorage); ok && n > 0 {
		rs.blockSize = n
	}
}

// Put uploads a block, then (when fillOnUpload) fire-and-forget pushes the
// bytes to the block's ring owner. The upload path is never blocked or
// failed by the push (best-effort, enterprise parity).
func (s *ringStorage) Put(ctx context.Context, key string, in io.Reader, getters ...object.AttrGetter) error {
	if !s.fillOnUpload {
		return s.inner.Put(ctx, key, in, getters...)
	}
	// Buffer so we can both hand inner.Put a reader and push the same bytes.
	// Blocks are bounded (maxBlockPayload); larger streams pass through
	// unbuffered and un-pushed.
	data, rerr := io.ReadAll(io.LimitReader(in, maxBlockPayload+1))
	if rerr != nil {
		return rerr
	}
	var pushable []byte
	if len(data) <= maxBlockPayload {
		pushable = data
	}
	if err := s.inner.Put(ctx, key, bytes.NewReader(data), getters...); err != nil {
		return err
	}
	if pushable != nil {
		members := s.members(s.group)
		if len(members) > 0 {
			go s.rc.Push(context.Background(), s.group, key, pushable, members)
		}
	}
	return nil
}

// Delete removes the object, then fire-and-forget broadcasts a drop so ring
// copies are evicted (enterprise 5.0 parity).
func (s *ringStorage) Delete(ctx context.Context, key string, getters ...object.AttrGetter) error {
	if err := s.inner.Delete(ctx, key, getters...); err != nil {
		return err
	}
	members := s.members(s.group)
	if len(members) > 0 {
		go s.rc.Drop(context.Background(), s.group, key, members)
	}
	return nil
}

// NewRingStorage wraps inner with ring-aware Get interception. members must
// return the live peer list for its argument group; for v1 the wiring passes
// a closure bound to the single configured group (call it with that group —
// the decorator forwards its own bound group name).
func NewRingStorage(inner object.ObjectStorage, rc *RingClient, members func(group string) []Member, selfUUID string) object.ObjectStorage {
	return &ringStorage{
		passthroughStorage: passthroughStorage{inner: inner},
		rc:                 rc,
		group:              "", // v1: wiring binds the single group in members()
		members:            members,
		selfUUID:           selfUUID,
	}
}

func (s *ringStorage) Get(ctx context.Context, key string, off, limit int64, getters ...object.AttrGetter) (io.ReadCloser, error) {
	fullBlock := off == 0 && (limit == -1 || (s.blockSize > 0 && limit == s.blockSize))
	if !fullBlock || isServerFill(ctx) {
		return s.inner.Get(ctx, key, off, limit, getters...)
	}
	members := s.members(s.group)
	if len(members) == 0 {
		logger.Debugf("gcache no members for group %q, reading from storage", s.group)
		return s.inner.Get(ctx, key, off, limit, getters...)
	}
	owner := Owners(key, members, 1)
	if len(owner) == 0 || owner[0].UUID == s.selfUUID {
		logger.Debugf("gcache %s self-owned by %s, reading from storage", key, s.selfUUID)
		return s.inner.Get(ctx, key, off, limit, getters...)
	}
	logger.Debugf("gcache peer fetch %s from %s", key, owner[0].Addr)
	r, err := s.rc.Fetch(ctx, s.group, key, members)
	if err != nil {
		// Peer fetch failed (error frame, timeout, or eviction): the client
		// always stays serviceable — fall through to object storage. The
		// block belongs to another member, so strict one-copy sharding
		// keeps the fallback bytes out of the local persistent cache.
		logger.Debugf("gcache peer fetch %s failed, falling back: %v", key, err)
		in, gerr := s.inner.Get(ctx, key, off, limit, getters...)
		if gerr != nil {
			return nil, gerr
		}
		return noCacheReader{ReadCloser: in}, nil
	}
	pr, ok := r.(*payloadReader)
	if !ok {
		return r, nil // defensive: unexpected reader type, serve as-is
	}
	return &peerReader{
		cur:   pr,
		total: pr.plen,
		refill: func(consumed int64) (io.ReadCloser, error) {
			in, gerr := s.inner.Get(ctx, key, 0, -1, getters...)
			if gerr != nil {
				return nil, gerr
			}
			if consumed > 0 {
				if _, err := io.CopyN(io.Discard, in, consumed); err != nil {
					_ = in.Close()
					return nil, fmt.Errorf("skip %d consumed bytes of %s: %w", consumed, key, err)
				}
			}
			return in, nil
		},
	}, nil
}
