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
	"context"
	"io"
	"net"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
)

// ServeConn handles one peer connection (CONTRACT.md §6 frames).
// TODO(gcache-worker): real frame loop — parse MsgBlockReq, serve from src
// (LoadCached → FillFromStorage on os.ErrNotExist), stream MsgBlockResp or
// MsgError, honor ctx shutdown. Placeholder: drains and closes politely.
func ServeConn(ctx context.Context, conn net.Conn, src ServerSource, timeout time.Duration) {
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	buf := make([]byte, headerLen)
	for {
		if _, err := io.ReadFull(conn, buf); err != nil {
			return
		}
		// Stub: reject every request with an error frame until the real
		// server lands. Wire shape is exercised by client/server tests.
		_ = conn
		return
	}
}

// NewRingStorage wraps inner with ring-aware Get interception: full-block
// Gets (off==0 && limit==-1) for peer-owned blocks are served by the owner;
// everything else falls through untouched (CONTRACT.md §2).
// TODO(gcache-worker): real implementation. Placeholder: pure passthrough,
// which is correct behavior when members is empty.
func NewRingStorage(inner object.ObjectStorage, rc interface{}, members func(group string) []Member, selfUUID string) object.ObjectStorage {
	return &passthroughStorage{inner: inner}
}

type passthroughStorage struct {
	inner object.ObjectStorage
}

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
