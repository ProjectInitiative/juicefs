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

import (
	"fmt"
	"os"

	"github.com/juicedata/juicefs/pkg/fs"
	"github.com/juicedata/juicefs/pkg/meta"
)

// FSBackend adapts *fs.FileSystem to the Backend interface. It reuses the
// mount's live metadata client and chunk store: cache-group routing, local
// caches, and peer ranged reads all apply exactly as for FUSE reads.
type FSBackend struct{ FS *fs.FileSystem }

// Open opens path read-only and returns the handle plus its size.
func (b *FSBackend) Open(ctx meta.Context, path string) (handleFile, int64, error) {
	f, errno := b.FS.Open(ctx, path, uint32(os.O_RDONLY))
	if errno != 0 {
		return nil, 0, fmt.Errorf("open %s: %s", path, errno)
	}
	st, errno := b.FS.Stat(ctx, path)
	if errno != 0 {
		_ = f.Close(ctx)
		return nil, 0, fmt.Errorf("stat %s: %s", path, errno)
	}
	return f, st.Size(), nil
}
