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

//go:build linux

package readsvc

import (
	"net"
	"syscall"
)

// peerCreds returns the peer's uid/gid/pid via SO_PEERCRED (Linux unix
// sockets). ok=false for anything that cannot be authenticated; the server
// refuses unauthenticated transports rather than serving as root.
func peerCreds(conn net.Conn) (uid, gid, pid uint32, ok bool) {
	uc, isUnix := conn.(*net.UnixConn)
	if !isUnix {
		return 0, 0, 0, false
	}
	raw, err := uc.SyscallConn()
	if err != nil {
		return 0, 0, 0, false
	}
	var ucred *syscall.Ucred
	var ctlErr error
	if err := raw.Control(func(fd uintptr) {
		ucred, ctlErr = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	}); err != nil {
		return 0, 0, 0, false
	}
	if ctlErr != nil || ucred == nil {
		return 0, 0, 0, false
	}
	return ucred.Uid, ucred.Gid, uint32(ucred.Pid), true
}

// peerCredsSupported reports whether the build can authenticate peers.
func peerCredsSupported() bool { return true }
