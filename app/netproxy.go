package main

// netproxy is a TCP proxy that adds a fixed one-way delay in each direction.
// It emulates the network between cloud services (for example, across AWS
// availability zones). One request/response exchange gets 2 x NET_DELAY_US.
//
// Why not tc netem or Toxiproxy: the Docker Desktop VM rounds every timer to
// about 3 ms, so sleep-based tools add 3-5 ms instead of 0.5 ms.
//
// Design:
//   - Reader goroutines timestamp each chunk and put it on one FIFO queue.
//     The delay is constant, so FIFO order is also deadline order.
//   - One dispatcher thread spin-waits until the head chunk is due (accurate
//     to a few microseconds) and writes it to the destination socket with one
//     NON-BLOCKING write system call. No goroutine wake-up is needed.
//   - If the socket buffer is full, the rest goes to the writer goroutine of
//     that connection. A slow socket never blocks the dispatcher.
//
// ROUTES="5432=postgres:5432,6379=redis:6379" maps listen ports to upstreams.

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"
)

type delayedChunk struct {
	buf []byte // nil = end of stream
	due time.Time
	out *outQueue
}

// outQueue holds the data for one direction of one connection.
type outQueue struct {
	mu      sync.Mutex
	items   [][]byte // data the writer goroutine must send, in order; nil = end of stream
	writing bool     // the writer goroutine is sending items now
	failed  bool
	raw     syscall.RawConn
	src     net.Conn
	signal  chan struct{} // capacity 1
}

// deliver is called by the dispatcher thread. It never blocks on I/O.
func (q *outQueue) deliver(b []byte) {
	q.mu.Lock()
	if b != nil && q.failed {
		q.mu.Unlock()
		return // drop data after a write error
	}
	if b != nil && !q.writing && len(q.items) == 0 {
		n, err := q.writeNonBlocking(b)
		if err == nil && n == len(b) {
			q.mu.Unlock()
			return
		}
		if err != nil && !errors.Is(err, syscall.EAGAIN) && !errors.Is(err, syscall.EINTR) {
			q.failed = true
			q.mu.Unlock()
			q.src.Close() // stops the reader, which then sends the end-of-stream marker
			return
		}
		b = b[n:]
	}
	q.items = append(q.items, b)
	q.mu.Unlock()
	q.wake()
}

func (q *outQueue) writeNonBlocking(b []byte) (int, error) {
	var n int
	var werr error
	if err := q.raw.Write(func(fd uintptr) bool {
		n, werr = syscall.Write(int(fd), b)
		return true // do not wait for the socket to become writable
	}); err != nil {
		return 0, err
	}
	if n < 0 {
		n = 0
	}
	return n, werr
}

func (q *outQueue) wake() {
	select {
	case q.signal <- struct{}{}:
	default:
	}
}

func runNetProxy(ctx context.Context) {
	delay := time.Duration(envInt("NET_DELAY_US", 500)) * time.Microsecond
	queue := make(chan delayedChunk, 1<<16)
	go dispatch(queue)

	for _, route := range strings.Split(env("ROUTES", ""), ",") {
		parts := strings.SplitN(strings.TrimSpace(route), "=", 2)
		if len(parts) != 2 {
			log.Fatalf("invalid route %q", route)
		}
		ln, err := net.Listen("tcp", ":"+parts[0])
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("netproxy :%s -> %s, one-way delay %v", parts[0], parts[1], delay)
		go acceptLoop(ln, parts[1], delay, queue)
	}
	<-ctx.Done()
}

func dispatch(queue <-chan delayedChunk) {
	runtime.LockOSThread()
	for c := range queue {
		if d := time.Until(c.due); d > 5*time.Millisecond {
			time.Sleep(d - 4*time.Millisecond) // coarse sleep only for long delays
		}
		for time.Now().Before(c.due) {
			// spin: sub-millisecond precision
		}
		c.out.deliver(c.buf)
	}
}

func acceptLoop(ln net.Listener, upstream string, delay time.Duration, queue chan<- delayedChunk) {
	for {
		client, err := ln.Accept()
		if err != nil {
			log.Printf("accept: %v", err)
			continue
		}
		go func() {
			server, err := net.DialTimeout("tcp", upstream, 5*time.Second)
			if err != nil {
				log.Printf("dial %s: %v", upstream, err)
				client.Close()
				return
			}
			var wg sync.WaitGroup
			wg.Add(2)
			go pipe(client.(*net.TCPConn), server.(*net.TCPConn), delay, queue, &wg)
			go pipe(server.(*net.TCPConn), client.(*net.TCPConn), delay, queue, &wg)
			wg.Wait()
			client.Close()
			server.Close()
		}()
	}
}

// pipe copies src -> dst through the delay queue.
func pipe(src, dst *net.TCPConn, delay time.Duration, queue chan<- delayedChunk, wg *sync.WaitGroup) {
	defer wg.Done()
	raw, err := dst.SyscallConn()
	if err != nil {
		log.Printf("syscall conn: %v", err)
		src.Close()
		return
	}
	out := &outQueue{raw: raw, src: src, signal: make(chan struct{}, 1)}
	writerDone := make(chan struct{})
	go writer(out, dst, writerDone)

	buf := make([]byte, 64*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			queue <- delayedChunk{buf: chunk, due: time.Now().Add(delay), out: out}
		}
		if err != nil {
			if err != io.EOF && !errors.Is(err, net.ErrClosed) {
				log.Printf("read: %v", err)
			}
			break
		}
	}
	queue <- delayedChunk{buf: nil, due: time.Now().Add(delay), out: out}
	<-writerDone
}

// writer sends the data that the dispatcher could not write without blocking.
// It stops after the end-of-stream marker.
func writer(q *outQueue, dst *net.TCPConn, done chan<- struct{}) {
	defer close(done)
	for range q.signal {
		q.mu.Lock()
		items := q.items
		q.items = nil
		q.writing = len(items) > 0
		failed := q.failed
		q.mu.Unlock()

		var bufs net.Buffers
		end := false
		for _, b := range items {
			if b == nil {
				end = true
				break
			}
			bufs = append(bufs, b)
		}
		if !failed && len(bufs) > 0 {
			if _, err := bufs.WriteTo(dst); err != nil {
				q.mu.Lock()
				q.failed = true
				q.mu.Unlock()
				q.src.Close()
			}
		}

		q.mu.Lock()
		q.writing = false
		more := len(q.items) > 0
		q.mu.Unlock()

		if end {
			_ = dst.CloseWrite()
			return
		}
		if more {
			q.wake()
		}
	}
}
