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

// rdmaecho — the Go/cgo rsockets adapter isolated from JuiceFS. Speaks the
// same wire protocol as rs_echo.c (8-byte "REQ1"+nonce request, 4096-byte
// response echoing the nonce) so (C|Go client) x (C|Go server) all
// interoperate. Phase-1 discriminator: if only the Go side stalls, the bug
// is in the adapter; if C stalls too, it is below.
//
//	rdmaecho server --port P
//	rdmaecho client --addr H:P --conns N --secs S [--stall-ms 3000]

package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juicedata/juicefs/pkg/gcache"
)

const (
	echoMagic = 0x31514552 // "REQ1" LE — same as rs_echo.c
	echoPay   = 4096
	echoSeed  = 0x12345678
)

func echoFill(p []byte, nonce uint32) {
	x := nonce ^ echoSeed
	for i := 4; i < len(p); i++ {
		x = x*1664525 + 1013904223
		p[i] = byte(x >> 24)
	}
	binary.LittleEndian.PutUint32(p, nonce)
}

func init() {
	rdmaEchoAvailable = true
	rdmaEchoCmd = cmdRdmaEcho
}

func cmdRdmaEcho(args []string) {
	fs := newFlagSet("rdmaecho")
	mode := fs.String("mode", "client", "server | client")
	addr := fs.String("addr", "", "server address for --mode client")
	port := fs.Int("port", 13990, "listen/connect port")
	conns := fs.Int("conns", 64, "simultaneous connections (client)")
	secs := fs.Int("secs", 10, "duration (client)")
	stallMS := fs.Int("stall-ms", 3000, "per-request deadline that counts as a stall (client)")
	_ = fs.Parse(args)

	switch *mode {
	case "server":
		rdmaEchoServer(*port)
	case "client":
		if *addr == "" {
			fatalf("--addr required")
		}
		rdmaEchoClient(*addr, *port, *conns, *secs, *stallMS)
	default:
		fatalf("unknown --mode %q", *mode)
	}
}

func rdmaEchoServer(port int) {
	rt, err := gcache.ListenRDMA(nil, ":"+strconv.Itoa(port))
	if err != nil {
		fatalf("rdma listen: %v", err)
	}
	ln, addr := rt, rt.Addr()
	if err != nil {
		fatalf("rdma listen: %v", err)
	}
	fmt.Printf("rs_echo-compatible Go server on %s (rsocket)\n", addr)
	var served, errs atomic.Int64
	go func() {
		for range time.Tick(5 * time.Second) {
			fmt.Printf("[srv] served=%d errors=%d goroutines=%d\n",
				served.Load(), errs.Load(), runtime.NumGoroutine())
		}
	}()
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go func(c net.Conn) {
			defer c.Close()
			req := make([]byte, 8)
			out := make([]byte, echoPay)
			for {
				if err := readFull(c, req); err != nil {
					if err != ioEOF {
						errs.Add(1)
					}
					return
				}
				if binary.LittleEndian.Uint32(req) != echoMagic {
					errs.Add(1)
					return
				}
				nonce := binary.LittleEndian.Uint32(req[4:])
				echoFill(out, nonce)
				if _, err := c.Write(out); err != nil {
					errs.Add(1)
					return
				}
				served.Add(1)
			}
		}(conn)
	}
}

var ioEOF = fmt.Errorf("EOF")

func readFull(c net.Conn, buf []byte) error {
	got := 0
	for got < len(buf) {
		n, err := c.Read(buf[got:])
		if n > 0 {
			got += n
		}
		if err != nil {
			if err.Error() == "EOF" {
				return ioEOF
			}
			return err
		}
		if n == 0 {
			return fmt.Errorf("zero-length read")
		}
	}
	return nil
}

func rdmaEchoClient(host string, port, conns, secs, stallMS int) {
	target := net.JoinHostPort(host, strconv.Itoa(port))
	// warm sanity dial
	tr, err := gcache.DialRDMA()
	if err != nil {
		fatalf("--rdma unavailable: %v", err)
	}
	c0, err := tr.Dial(context.Background(), target)
	if err != nil {
		fatalf("dial %s: %v", target, err)
	}
	_ = c0.Close()

	var ok, timeouts, errs atomic.Int64
	var stallsMu sync.Mutex
	var stallStacks []string
	var latMu sync.Mutex
	var lat []float64
	legStart := time.Now()

	var wg sync.WaitGroup
	for i := 0; i < conns; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			conn, err := tr.Dial(context.Background(), target)
			if err != nil {
				errs.Add(1)
				fmt.Printf("[cli %d] dial: %v\n", idx, err)
				return
			}
			defer conn.Close()
			req := make([]byte, 8)
			in := make([]byte, echoPay)
			nonce := uint32(idx*7919 + 13)
			deadline := time.Now().Add(time.Duration(secs) * time.Second)
			stallReported := false
			for time.Now().Before(deadline) {
				binary.LittleEndian.PutUint32(req, echoMagic)
				binary.LittleEndian.PutUint32(req[4:], nonce)
				t0 := time.Now()
				_ = conn.SetDeadline(time.Now().Add(time.Duration(stallMS) * time.Millisecond))
				if _, err := conn.Write(req); err != nil {
					errs.Add(1)
					return
				}
				if err := readFullDeadline(conn, in, stallReported, &stallsMu, &stallStacks, idx); err != nil {
					if isDeadline(err) {
						timeouts.Add(1)
						if !stallReported {
							stallReported = true
							buf := make([]byte, 1<<16)
							n := runtime.Stack(buf, true)
							stallsMu.Lock()
							stallStacks = append(stallStacks, fmt.Sprintf("[cli %d] stall at %s\n%s", idx, time.Now().Format(time.RFC3339), buf[:n]))
							stallsMu.Unlock()
						}
						return // the stream may be desynced: the worker exits
					}
					errs.Add(1)
					return
				}
				_ = conn.SetDeadline(time.Time{})
				if got := binary.LittleEndian.Uint32(in); got != nonce {
					errs.Add(1)
					fmt.Printf("[cli %d] NONCE MISMATCH sent=%d got=%d\n", idx, nonce, got)
					return
				}
				latMu.Lock()
				lat = append(lat, float64(time.Since(t0).Microseconds()))
				latMu.Unlock()
				ok.Add(1)
				nonce += 9973
			}
		}(i)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	tick := time.Tick(time.Second)
	for {
		select {
		case <-done:
			reportRdmaEcho(lat, &ok, &timeouts, &errs, conns, false, stallsMu, &stallStacks)
			return
		case <-tick:
			fmt.Printf("[cli %6.1fs] ok=%d timeouts=%d errors=%d goroutines=%d\n",
				time.Since(legStart).Seconds(), ok.Load(), timeouts.Load(), errs.Load(), runtime.NumGoroutine())
		case <-time.After(time.Duration(secs+stallMS/1000+20) * time.Second):
			reportRdmaEcho(lat, &ok, &timeouts, &errs, conns, true, stallsMu, &stallStacks)
			return
		}
	}
}

// readFullDeadline reads exactly len(buf) with the conn's deadline; on the
// FIRST stall it captures all goroutine stacks.
func readFullDeadline(c net.Conn, buf []byte, _ bool, mu *sync.Mutex, stacks *[]string, idx int) error {
	got := 0
	for got < len(buf) {
		n, err := c.Read(buf[got:])
		if n > 0 {
			got += n
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("zero-length read")
		}
		if got == 0 {
			// capture stacks once at the first sign of a hang
			mu.Lock()
			if len(*stacks) == 0 {
				buf := make([]byte, 1<<16)
				n2 := runtime.Stack(buf, true)
				*stacks = append(*stacks, fmt.Sprintf("[cli %d] first hang\n%s", idx, buf[:n2]))
			}
			mu.Unlock()
		}
	}
	return nil
}

func isDeadline(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}
	return strings.Contains(err.Error(), "timeout") || strings.Contains(err.Error(), "deadline")
}

func reportRdmaEcho(lat []float64, ok, timeouts, errs *atomic.Int64, conns int, killed bool, mu sync.Mutex, stacks *[]string) {
	sort.Float64s(lat)
	if len(lat) > 0 {
		fmt.Printf("[cli result] reads=%d p50=%.0fus p95=%.0fus p99=%.0fus max=%.0fus killed=%v\n",
			len(lat), lat[len(lat)/2], lat[int(float64(len(lat))*.95)],
			lat[int(float64(len(lat))*.99)], lat[len(lat)-1], killed)
	} else {
		fmt.Printf("[cli result] reads=0 — TOTAL STALL (killed=%v)\n", killed)
	}
	fmt.Printf("[cli result] conns=%d timeouts=%d errors=%d\n", conns, timeouts.Load(), errs.Load())
	mu.Lock()
	for i, s := range *stacks {
		if i >= 2 {
			break
		}
		fmt.Printf("=== STALL STACK %d ===\n%s\n", i, s)
	}
	mu.Unlock()
}
