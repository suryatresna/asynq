// Copyright 2026 The asynq Authors. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"go.uber.org/goleak"
)

// miniredisServer is the in-process Redis used when no real Redis is reachable.
// Set by TestMain so all tests in this package share a single instance.
var miniredisServer *miniredis.Miniredis

// TestMain probes the configured Redis address and, if it is unreachable, falls
// back to an in-process miniredis instance for the duration of the test run.
// This lets `go test ./...` succeed on machines that don't have a Redis server
// running locally, without forcing every test to know about the fallback.
func TestMain(m *testing.M) {
	flag.Parse()

	if !useRedisCluster && !canReachRedis(redisAddr) {
		mr, err := miniredis.Run()
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to start miniredis: %v\n", err)
			os.Exit(1)
		}
		miniredisServer = mr
		redisAddr = mr.Addr()
		// miniredis only supports DB 0; pin the test DB index to match.
		redisDB = 0
		fmt.Fprintf(os.Stderr, "no real Redis at the configured address; using miniredis at %s\n", mr.Addr())
	}

	code := m.Run()

	if miniredisServer != nil {
		miniredisServer.Close()
	}
	os.Exit(code)
}

// goleakIgnoreOpts returns goleak.Option values that should be passed to
// goleak.VerifyNone in tests that interact with Redis. It always ignores the
// go-redis connection reaper, and when we're running against an in-process
// miniredis server, it also ignores miniredis' connection-handling goroutines
// (those goroutines stay alive for the lifetime of the test binary and are
// not leaks from asynq's code under test).
func goleakIgnoreOpts() []goleak.Option {
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/redis/go-redis/v9/internal/pool.(*ConnPool).reaper"),
	}
	if miniredisServer != nil {
		opts = append(opts,
			goleak.IgnoreAnyFunction("github.com/alicebob/miniredis/v2/server.(*Server).serve"),
			goleak.IgnoreAnyFunction("github.com/alicebob/miniredis/v2/server.newServer.func1"),
			goleak.IgnoreAnyFunction("github.com/alicebob/miniredis/v2/server.(*Server).servePeer"),
			goleak.IgnoreAnyFunction("github.com/alicebob/miniredis/v2/server.(*Server).servePeer.func1"),
			goleak.IgnoreAnyFunction("github.com/alicebob/miniredis/v2/server.(*Server).servePeer.func2"),
			goleak.IgnoreAnyFunction("github.com/alicebob/miniredis/v2/server.(*Server).ServeConn.func1"),
			goleak.IgnoreAnyFunction("github.com/alicebob/miniredis/v2/server.readArray"),
		)
	}
	return opts
}

// canReachRedis returns true if a PING succeeds against addr within a short
// timeout. We don't care about auth or DB selection here — just whether the
// server is listening.
func canReachRedis(addr string) bool {
	c := redis.NewClient(&redis.Options{
		Addr:        addr,
		DialTimeout: 500 * time.Millisecond,
		ReadTimeout: 500 * time.Millisecond,
	})
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	return c.Ping(ctx).Err() == nil
}
