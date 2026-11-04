package simnet

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

// echoServer replies to every line with "echo: <line>".
func echoServer(ln net.Listener) {
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		go func() {
			defer c.Close()
			r := bufio.NewReader(c)
			for {
				line, err := r.ReadString('\n')
				if err != nil {
					return
				}
				fmt.Fprintf(c, "echo: %s", line)
			}
		}()
	}
}

func TestEchoOverSimulatedNetwork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		nw := New(Config{Seed: 1, MinLatency: 5 * time.Millisecond, MaxLatency: 10 * time.Millisecond})
		ln, err := nw.Host("server").Listen(7000)
		if err != nil {
			t.Fatal(err)
		}
		go echoServer(ln)
		defer nw.Close()

		c, err := nw.Host("client").Dial(context.Background(), "server:7000")
		if err != nil {
			t.Fatal(err)
		}
		r := bufio.NewReader(c)
		for i := 0; i < 5; i++ {
			start := time.Now()
			fmt.Fprintf(c, "hello %d\n", i)
			got, err := r.ReadString('\n')
			if err != nil {
				t.Fatal(err)
			}
			if want := fmt.Sprintf("echo: hello %d\n", i); got != want {
				t.Fatalf("got %q, want %q", got, want)
			}
			// One round trip: two one-way latencies in [5ms, 10ms+1µs).
			if rtt := time.Since(start); rtt < 10*time.Millisecond || rtt > 21*time.Millisecond {
				t.Fatalf("round trip took %v of fake time", rtt)
			}
		}
	})
}

func TestManyWritesArriveInOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		nw := New(Config{Seed: 2, MinLatency: time.Millisecond, MaxLatency: 50 * time.Millisecond})
		ln, _ := nw.Host("server").Listen(1)
		got := make(chan string, 1)
		go func() {
			c, _ := ln.Accept()
			b, _ := io.ReadAll(c)
			got <- string(b)
		}()
		c, _ := nw.Host("client").Dial(context.Background(), "server:1")
		var want strings.Builder
		for i := 0; i < 200; i++ {
			s := fmt.Sprintf("%d,", i)
			want.WriteString(s)
			c.Write([]byte(s)) // random latencies, but TCP-like order
		}
		c.Close()
		if g := <-got; g != want.String() {
			t.Fatalf("bytes reordered or lost:\n got %.80s...\nwant %.80s...", g, want.String())
		}
		nw.Close()
	})
}

func TestPartitionHoldsDataUntilHeal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		nw := New(Config{Seed: 3})
		ln, _ := nw.Host("b").Listen(1)
		accepted := make(chan net.Conn, 1)
		go func() {
			c, _ := ln.Accept()
			accepted <- c
		}()
		c, _ := nw.Host("a").Dial(context.Background(), "b:1")
		srv := <-accepted

		nw.Partition([]string{"a"}, []string{"b"})
		c.Write([]byte("during partition"))

		srv.SetReadDeadline(time.Now().Add(time.Second))
		buf := make([]byte, 64)
		if _, err := srv.Read(buf); !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("read during partition: got %v, want deadline exceeded", err)
		}

		nw.Heal()
		srv.SetReadDeadline(time.Time{})
		n, err := srv.Read(buf)
		if err != nil || string(buf[:n]) != "during partition" {
			t.Fatalf("after heal: %q, %v", buf[:n], err)
		}
		nw.Close()
	})
}

func TestCrashClosesConnectionsAndListeners(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		nw := New(Config{Seed: 4})
		server := nw.Host("server")
		ln, _ := server.Listen(1)
		acceptErr := make(chan error, 1)
		go func() {
			for {
				c, err := ln.Accept()
				if err != nil {
					acceptErr <- err
					return
				}
				_ = c
			}
		}()
		c, _ := nw.Host("client").Dial(context.Background(), "server:1")
		synctest.Wait()

		server.Crash()
		if err := <-acceptErr; !errors.Is(err, net.ErrClosed) {
			t.Fatalf("accept after crash: %v", err)
		}
		if _, err := c.Read(make([]byte, 1)); err != io.EOF {
			t.Fatalf("client read after server crash: %v, want EOF", err)
		}
		// Dialing a crashed host times out.
		start := time.Now()
		if _, err := nw.Host("client").Dial(context.Background(), "server:1"); err == nil {
			t.Fatal("dial to a crashed host succeeded")
		}
		if d := time.Since(start); d != 3*time.Second {
			t.Fatalf("dial timeout took %v, want 3s", d)
		}
		server.Restart()
		if _, err := server.Listen(1); err != nil {
			t.Fatalf("listen after restart: %v", err)
		}
		nw.Close()
	})
}

func TestDialRefusedWithoutListener(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		nw := New(Config{Seed: 5})
		_, err := nw.Host("a").Dial(context.Background(), "b:80")
		if err == nil || !strings.Contains(err.Error(), "refused") {
			t.Fatalf("got %v, want connection refused", err)
		}
	})
}
