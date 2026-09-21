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
	"os"
	"testing"
)

// Contract tests (CONTRACT.md §10): detect upstream refactors that would
// break pkg/gcache. Run with: go test ./pkg/gcache/ -run TestContract

// §4: ServerSource must be satisfiable by a minimal adapter (structural
// typing contract with pkg/chunk/gcache_bridge.go).
type testSource struct{}

func (testSource) LoadCached(string) (io.ReadCloser, error) {
	return nil, os.ErrNotExist
}
func (testSource) FillFromStorage(_ context.Context, _ string) (io.ReadCloser, error) {
	return nil, os.ErrNotExist
}
func (testSource) CompressAlgo() string    { return "none" }
func (testSource) CompressBound(n int) int { return n }
func (testSource) CompressPayload(dst, src []byte) (int, error) {
	return copy(dst, src), nil
}

func TestContractServerSource(t *testing.T) {
	var _ ServerSource = testSource{}
}

// §6: header length and magic bytes are load-bearing.
func TestContractWireHeader(t *testing.T) {
	if headerLen != 20 {
		t.Fatalf("headerLen changed: %d", headerLen)
	}
	if magic0 != 'G' || magic1 != 'C' || protoVer != 1 {
		t.Fatalf("magic/proto changed")
	}
}

// §5: Member must round-trip through JSON for the registry.
func TestContractMemberJSON(t *testing.T) {
	// covered fully in ring_test/registry_test; here just pin the field set
	m := Member{UUID: "u", Addr: "h:1", Weight: 2, Version: "v", TS: 3}
	if m.UUID != "u" || m.Addr != "h:1" || m.Weight != 2 || m.Version != "v" || m.TS != 3 {
		t.Fatal("member fields changed")
	}
}
