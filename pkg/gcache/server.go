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
		if hdr[0] != magic0 || hdr[1] != magic1 || hdr[2] != protoVer || hdr[3] != MsgBlockReq {
			_ = sendError(ctx, conn, errors.New("bad request header"))
			return
		}
		klen := int(binary.LittleEndian.Uint16(hdr[9:11]))
		if klen == 0 || klen > 4096 {
			_ = sendError(ctx, conn, errors.New("invalid key length"))
			return
		}
		key := make([]byte, klen)
		if _, err := io.ReadFull(conn, key); err != nil {
			return
		}
		// Reset deadline for the (potentially large) block transfer.
		_ = conn.SetDeadline(time.Now().Add(timeout))
		if err := serveBlock(ctx, conn, src, string(key)); err != nil {
			logger.Debugf("gcache serve %s: %v", key, err)
		}
	}
}

// maxBlockPayload bounds ReadAll as a safety net against a corrupt source
// reporting a bogus size (blocks are ≤ 16MiB even at the largest setting).
const maxBlockPayload = 64 << 20

// serveBlock resolves one key and writes the response frame.
func serveBlock(ctx context.Context, conn net.Conn, src ServerSource, key string) error {
	r, err := src.LoadCached(key)
	if errors.Is(err, os.ErrNotExist) {
		r, err = src.FillFromStorage(withServerFill(ctx), key)
	}
	if err != nil {
		return sendError(ctx, conn, err)
	}
	defer r.Close()

	data, err := io.ReadAll(io.LimitReader(r, maxBlockPayload+1))
	if err != nil {
		return sendError(ctx, conn, fmt.Errorf("read cached block: %w", err))
	}
	if len(data) > maxBlockPayload {
		return sendError(ctx, conn, errors.New("block exceeds payload limit"))
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
			return sendError(ctx, conn, fmt.Errorf("compress block: %w", cerr))
		}
		algo = WireCompressed
		payload = bytes.NewReader(dst[:n])
	}

	if err := writeResp(ctx, conn, algo, payload); err != nil {
		return fmt.Errorf("write response: %w", err)
	}
	return nil
}

// writeResp writes a MsgBlockResp header followed by the payload stream.
func writeResp(ctx context.Context, conn net.Conn, algo uint8, payload *bytes.Reader) error {
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
	_, err := io.CopyN(writerOnly{conn}, payload, int64(payload.Len()))
	return err
}

// sendError writes a MsgError frame: plen=^uint64(0), then klen+msg.
func sendError(ctx context.Context, conn net.Conn, cause error) error {
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

// writerOnly hides io.ReaderFrom on net.Conn so io.CopyN cannot claim a
// zero-copy path that ignores the context wrapper.
type writerOnly struct{ w io.Writer }

func (wo writerOnly) Write(p []byte) (int, error) { return wo.w.Write(p) }

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
// (off==0 && limit==-1, the only shape cachedStore.load() uses — CONTRACT
// §2) and serves ring-owned blocks from the owning peer; everything else
// falls through to the wrapped ObjectStorage untouched.
type ringStorage struct {
	passthroughStorage
	rc       *RingClient
	group    string // registry group whose members() closure is wired
	members  func(group string) []Member
	selfUUID string
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
	if off != 0 || limit != -1 || isServerFill(ctx) {
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
		// always stays serviceable — fall through to object storage.
		logger.Debugf("gcache peer fetch %s failed, falling back: %v", key, err)
		return s.inner.Get(ctx, key, off, limit, getters...)
	}
	return r, nil
}
