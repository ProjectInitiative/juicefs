//go:build rdma

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

// rsockets (librdmacm) transport — the real RDMA datapath behind --rdma-network.
//
// Build:   go build -tags rdma ...   (requires rdma-core headers + librdmacm)
// Runtime: requires rdma-core userland and a working RDMA device (RoCE/IB or
//          rdma_rxe); verify with `rping -s` / `rping -c -a <peer>` first.
//
// rsockets keeps the byte-identical wire protocol (ServeConn takes net.Conn):
// this file only implements Dial/Listen over the rsocket API. rsocket fds are
// library-internal (not kernel fds), so all IO goes through rread/rwrite/
// rpoll — a net.Conn adapter with deadline support wraps them.
//
// Binaries built with this tag require librdmacm.so.1 at load time.

package gcache

/*
#cgo linux LDFLAGS: -lrdmacm
#include <rdma/rsocket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <poll.h>
#include <errno.h>
#include <string.h>

#ifndef SOCK_NONBLOCK
#define SOCK_NONBLOCK 04000
#endif

static int c_rsocket(int *saved_errno) {
	int fd = rsocket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
	if (fd < 0) *saved_errno = errno;
	return fd;
}
// ip4 points at the 4 address octets in network order; memcpy places them
// verbatim into sin_addr.s_addr, which is correct on any host endianness.
// (Passing a Go binary.BigEndian.Uint32 value instead stored the octets
// byte-swapped on little-endian hosts: 10.42.1.2 became 2.1.42.10 — review
// round 3, finding 3.)
static int c_rconnect(int fd, const unsigned char *ip4, int port, int *saved_errno) {
	struct sockaddr_in sa;
	memset(&sa, 0, sizeof sa);
	sa.sin_family = AF_INET;
	sa.sin_port = htons((unsigned short)port);
	memcpy(&sa.sin_addr.s_addr, ip4, 4);
	int rv = rconnect(fd, (struct sockaddr *)&sa, sizeof sa);
	if (rv < 0) *saved_errno = errno;
	return rv;
}
// actual bound port (host byte order) via rgetsockname; -1 on error.
static int c_rgetsockname_port(int fd, int *saved_errno) {
	struct sockaddr_in sa;
	socklen_t l = sizeof sa;
	if (rgetsockname(fd, (struct sockaddr *)&sa, &l) < 0) {
		*saved_errno = errno;
		return -1;
	}
	return ntohs(sa.sin_port);
}
static int c_rbind(int fd, int port, int *saved_errno) {
	struct sockaddr_in sa;
	memset(&sa, 0, sizeof sa);
	sa.sin_family = AF_INET;
	sa.sin_port = htons((unsigned short)port);
	sa.sin_addr.s_addr = INADDR_ANY;
	int rv = rbind(fd, (struct sockaddr *)&sa, sizeof sa);
	if (rv < 0) *saved_errno = errno;
	return rv;
}
static int c_rlisten(int fd, int backlog, int *saved_errno) {
	int rv = rlisten(fd, backlog);
	if (rv < 0) *saved_errno = errno;
	return rv;
}
static int c_raccept(int fd, int *saved_errno) {
	struct sockaddr_in sa;
	socklen_t l = sizeof sa;
	int rv = raccept(fd, (struct sockaddr *)&sa, &l);
	if (rv < 0) *saved_errno = errno;
	return rv;
}
static int c_rread(int fd, void *buf, int n, int *saved_errno) {
	ssize_t r = rread(fd, buf, (size_t)n);
	if (r < 0) *saved_errno = errno;
	return (int)r;
}
static int c_rwrite(int fd, const void *buf, int n, int *saved_errno) {
	ssize_t r = rwrite(fd, buf, (size_t)n);
	if (r < 0) *saved_errno = errno;
	return (int)r;
}
// returns: 1 ready, 0 timeout, -1 error (errno saved); revents out.
static int c_rpoll(int fd, short events, int timeout_ms, short *revents, int *saved_errno) {
	struct pollfd p;
	memset(&p, 0, sizeof p);
	p.fd = fd;
	p.events = events;
	int rv = rpoll(&p, 1, timeout_ms);
	if (rv < 0) *saved_errno = errno;
	*revents = p.revents;
	return rv;
}
static int c_rsoerror(int fd, int *out, int *saved_errno) {
	socklen_t l = sizeof(*out);
	if (rgetsockopt(fd, SOL_SOCKET, SO_ERROR, out, &l) < 0) {
		*saved_errno = errno;
		return -1;
	}
	return 0;
}
static int c_rclose(int fd, int *saved_errno) {
	int rv = rclose(fd);
	if (rv < 0) *saved_errno = errno;
	return rv;
}
*/
import "C"

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

func init() {
	rdmaDialFn = dialRsockets
	rdmaListenFn = listenRsockets
}

const rsocketsBacklog = 128

// errOf converts a saved errno into an error (nil when 0).
func errOf(saved C.int) error {
	if saved == 0 {
		return nil
	}
	return syscall.Errno(saved)
}

// ---------------------------------------------------------------- dial

// ipv4Octets returns the 4 address octets in network order — exactly the
// memory layout sockaddr_in.sin_addr expects, independent of host
// endianness. Kept pure-Go so the byte-order contract has a regression test.
func ipv4Octets(host string) ([4]byte, error) {
	ip := net.ParseIP(host).To4()
	if ip == nil {
		return [4]byte{}, fmt.Errorf("%q is not an IPv4 address (rsockets v1)", host)
	}
	var out [4]byte
	copy(out[:], ip)
	return out, nil
}

func dialRsockets(ctx context.Context, addr string) (net.Conn, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("rsockets dial %s: %w", addr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port <= 0 || port > 65535 {
		return nil, fmt.Errorf("rsockets dial %s: bad port", addr)
	}
	oct, err := ipv4Octets(host)
	if err != nil {
		return nil, fmt.Errorf("rsockets dial: %w", err)
	}

	var serr C.int
	fd := C.c_rsocket(&serr)
	if fd < 0 {
		return nil, fmt.Errorf("rsocket: %w", errOf(serr))
	}
	closeFD := func() { var se C.int; _ = C.c_rclose(fd, &se) }

	rv := C.c_rconnect(fd, (*C.uchar)(unsafe.Pointer(&oct[0])), C.int(port), &serr)
	if rv < 0 {
		if errOf(serr) != syscall.EINPROGRESS {
			closeFD()
			return nil, fmt.Errorf("rconnect %s: %w", addr, errOf(serr))
		}
	}
	if err := waitWritable(fd, ctx, 10*time.Second); err != nil {
		closeFD()
		return nil, fmt.Errorf("rconnect %s: %w", addr, err)
	}
	var soerr C.int
	if C.c_rsoerror(fd, &soerr, &serr) < 0 {
		closeFD()
		return nil, fmt.Errorf("rconnect %s: getsockopt: %w", addr, errOf(serr))
	}
	if soerr != 0 {
		closeFD()
		return nil, fmt.Errorf("rconnect %s: %w", addr, syscall.Errno(soerr))
	}
	return &rsocketConn{fd: fd, remote: addr}, nil
}

// waitWritable polls POLLOUT until writable, ctx cancelation, or timeout.
func waitWritable(fd C.int, ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		slice := deadline.Sub(time.Now())
		if slice <= 0 {
			return errors.New("connect timeout")
		}
		if slice > 100*time.Millisecond {
			slice = 100 * time.Millisecond // keep ctx checks responsive
		}
		var revents C.short
		var serr C.int
		rv := C.c_rpoll(fd, C.POLLOUT, C.int(slice.Milliseconds()), &revents, &serr)
		if rv < 0 {
			if errOf(serr) == syscall.EINTR {
				continue
			}
			return errOf(serr)
		}
		if rv == 0 {
			continue
		}
		if revents&(C.POLLERR|C.POLLHUP|C.POLLNVAL) != 0 {
			var soerr C.int
			if C.c_rsoerror(fd, &soerr, &serr) == 0 && soerr > 0 {
				return syscall.Errno(soerr)
			}
			return errors.New("connect failed")
		}
		if revents&C.POLLOUT != 0 {
			return nil
		}
	}
}

// ---------------------------------------------------------------- listener

type rsocketsListener struct {
	fd     C.int
	port   int          // actual bound port
	addr   *net.TCPAddr // advertised
	closed atomic.Bool
}

func listenRsockets(nics []string, advertise string) (net.Listener, string, error) {
	var serr C.int
	fd := C.c_rsocket(&serr)
	if fd < 0 {
		return nil, "", fmt.Errorf("rsocket: %w", errOf(serr))
	}
	closeFD := func() { var se C.int; _ = C.c_rclose(fd, &se) }

	// Bind the requested port when the advertise address carries one;
	// otherwise let the kernel pick an ephemeral port. Either way the ACTUAL
	// bound port is discovered via rgetsockname and advertised (review
	// round 3, finding 2) — advertising a port that differs from the bound
	// socket is a guaranteed connect failure.
	advHost, advPort, _ := net.SplitHostPort(advertise)
	bindPort := 0
	if advPort != "" {
		p, err := strconv.Atoi(advPort)
		if err != nil || p <= 0 || p > 65535 {
			closeFD()
			return nil, "", fmt.Errorf("rbind: bad advertise port %q", advPort)
		}
		bindPort = p
	}
	if rv := C.c_rbind(fd, C.int(bindPort), &serr); rv < 0 {
		closeFD()
		return nil, "", fmt.Errorf("rbind: %w", errOf(serr))
	}
	actual := int(C.c_rgetsockname_port(fd, &serr))
	if actual < 0 {
		closeFD()
		return nil, "", fmt.Errorf("rgetsockname: %w", errOf(serr))
	}
	if bindPort != 0 && actual != bindPort {
		closeFD()
		return nil, "", fmt.Errorf("rbind: bound port %d does not match requested %d", actual, bindPort)
	}
	if rv := C.c_rlisten(fd, rsocketsBacklog, &serr); rv < 0 {
		closeFD()
		return nil, "", fmt.Errorf("rlisten: %w", errOf(serr))
	}
	host := ""
	if advHost != "" && advHost != "0.0.0.0" && advHost != "::" {
		host = advHost // wildcard bind hosts are not connectable: discover instead
	} else if h, err := firstNICIPv4(nics); err == nil {
		host = h
	} else {
		host = "127.0.0.1" // loopback fallback (single-node rxe testing)
	}
	addr := &net.TCPAddr{IP: net.ParseIP(host).To4(), Port: actual}
	ln := &rsocketsListener{fd: fd, port: actual, addr: addr}
	return ln, net.JoinHostPort(host, strconv.Itoa(actual)), nil
}

// firstNICIPv4 returns the first IPv4 address of the named NICs (or any
// non-loopback interface when nics is empty).
func firstNICIPv4(nics []string) (string, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	want := map[string]bool{}
	for _, n := range nics {
		want[n] = true
	}
	for _, iface := range ifaces {
		if len(nics) > 0 && !want[iface.Name] {
			continue
		}
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			if ipnet, ok := a.(*net.IPNet); ok && ipnet.IP.To4() != nil {
				return ipnet.IP.To4().String(), nil
			}
		}
	}
	return "", errors.New("no IPv4 address on requested NICs")
}

func (l *rsocketsListener) Accept() (net.Conn, error) {
	for {
		if l.closed.Load() {
			return nil, net.ErrClosed
		}
		var revents C.short
		var serr C.int
		rv := C.c_rpoll(l.fd, C.POLLIN, 100, &revents, &serr)
		if rv < 0 {
			if errOf(serr) == syscall.EINTR {
				continue
			}
			return nil, errOf(serr)
		}
		if rv == 0 {
			continue // 100ms slice: check the closed flag
		}
		if revents&C.POLLNVAL != 0 {
			return nil, net.ErrClosed
		}
		nfd := C.c_raccept(l.fd, &serr)
		if nfd < 0 {
			eno := errOf(serr)
			if eno == syscall.EAGAIN || eno == syscall.EWOULDBLOCK || eno == syscall.EINTR {
				continue
			}
			return nil, eno
		}
		return &rsocketConn{fd: nfd, local: l.addr.String()}, nil
	}
}

func (l *rsocketsListener) Close() error {
	if l.closed.Swap(true) {
		return nil
	}
	var serr C.int
	_, _ = C.c_rclose(l.fd, &serr)
	return nil
}

func (l *rsocketsListener) Addr() net.Addr { return l.addr }

// ---------------------------------------------------------------- conn

// rsocketConn adapts an rsocket fd to net.Conn with deadlines.
type rsocketConn struct {
	fd C.int

	mu        sync.Mutex
	rdeadline time.Time
	wdeadline time.Time

	local  string
	remote string
}

func (c *rsocketConn) waitReady(events C.short, deadline time.Time) error {
	for {
		var timeoutMs C.int = -1 // infinite
		if !deadline.IsZero() {
			d := time.Until(deadline)
			if d <= 0 {
				return os.ErrDeadlineExceeded
			}
			ms := int64(d / time.Millisecond)
			if ms < 1 {
				ms = 1
			}
			if ms > (int64(1)<<31 - 1) {
				ms = int64(1)<<31 - 1
			}
			timeoutMs = C.int(ms)
		}
		var revents C.short
		var serr C.int
		rv := C.c_rpoll(c.fd, events, timeoutMs, &revents, &serr)
		if rv < 0 {
			if errOf(serr) == syscall.EINTR {
				continue
			}
			return errOf(serr)
		}
		if rv == 0 {
			return os.ErrDeadlineExceeded
		}
		if revents&(C.POLLERR|C.POLLHUP|C.POLLNVAL) != 0 {
			return io.ErrClosedPipe
		}
		if revents&events != 0 {
			return nil
		}
	}
}

func (c *rsocketConn) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	for {
		if err := c.waitReady(C.POLLIN, c.readDeadline()); err != nil {
			return 0, err
		}
		var serr C.int
		n := C.c_rread(c.fd, unsafe.Pointer(&b[0]), C.int(len(b)), &serr)
		if n > 0 {
			return int(n), nil
		}
		if n < 0 {
			eno := errOf(serr)
			if eno == syscall.EINTR || eno == syscall.EAGAIN || eno == syscall.EWOULDBLOCK {
				continue
			}
			return 0, eno
		}
		return 0, io.EOF // orderly shutdown
	}
}

func (c *rsocketConn) Write(b []byte) (int, error) {
	written := 0
	for written < len(b) {
		if err := c.waitReady(C.POLLOUT, c.writeDeadline()); err != nil {
			return written, err
		}
		var serr C.int
		n := C.c_rwrite(c.fd, unsafe.Pointer(&b[written]), C.int(len(b)-written), &serr)
		if n > 0 {
			written += int(n)
			continue
		}
		if n < 0 {
			eno := errOf(serr)
			if eno == syscall.EINTR || eno == syscall.EAGAIN || eno == syscall.EWOULDBLOCK {
				continue
			}
			return written, eno
		}
		return written, io.ErrShortWrite
	}
	return written, nil
}

func (c *rsocketConn) Close() error {
	var serr C.int
	_, _ = C.c_rclose(c.fd, &serr)
	return nil
}

func (c *rsocketConn) LocalAddr() net.Addr  { return parseOrTCP(c.local) }
func (c *rsocketConn) RemoteAddr() net.Addr { return parseOrTCP(c.remote) }

func parseOrTCP(s string) net.Addr {
	if a, err := net.ResolveTCPAddr("tcp4", s); err == nil {
		return a
	}
	return &net.TCPAddr{}
}

func (c *rsocketConn) SetDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rdeadline, c.wdeadline = t, t
	return nil
}

func (c *rsocketConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rdeadline = t
	return nil
}

func (c *rsocketConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.wdeadline = t
	return nil
}

func (c *rsocketConn) readDeadline() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rdeadline
}

func (c *rsocketConn) writeDeadline() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.wdeadline
}
