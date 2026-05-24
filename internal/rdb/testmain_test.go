// Copyright 2026 The asynq Authors. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package rdb

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// miniredisServer is the in-process Redis used when no real Redis is reachable.
var miniredisServer *miniredis.Miniredis

// TestMain probes the configured Redis address and falls back to an in-process
// miniredis instance if it is unreachable. See the root package's testmain_test.go
// for the rationale.
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
		// miniredis only supports DB 0.
		redisDB = 0
		fmt.Fprintf(os.Stderr, "no real Redis at the configured address; using miniredis at %s\n", mr.Addr())
	}

	code := m.Run()

	if miniredisServer != nil {
		miniredisServer.Close()
	}
	os.Exit(code)
}

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
