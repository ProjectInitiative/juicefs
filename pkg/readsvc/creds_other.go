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

//go:build !linux

package readsvc

import "net"

// peerCreds: SO_PEERCRED is Linux-only. On other platforms the readsvc
// cannot authenticate callers and therefore refuses every connection
// (the service targets the Linux/DGX deployment).
func peerCreds(net.Conn) (uid, gid, pid uint32, ok bool) { return 0, 0, 0, false }

// peerCredsSupported reports whether the build can authenticate peers.
func peerCredsSupported() bool { return false }
