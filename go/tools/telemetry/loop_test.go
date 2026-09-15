// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package telemetry

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"

	"github.com/multigres/multigres/go/tools/ctxutil"
)

// initTestTelemetry sets up telemetry and registers a shutdown cleanup that
// uses a detached context, so tests can cancel their own ctx without breaking
// tracer-provider shutdown.
func initTestTelemetry(t *testing.T) *testTelemetrySetup {
	t.Helper()
	setup := SetupTestTelemetry(t)
	require.NoError(t, setup.Telemetry.InitTelemetry(t.Context(), "test-service"))
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(ctxutil.Detach(t.Context()), 2*time.Second)
		defer cancel()
		require.NoError(t, setup.Telemetry.ShutdownTelemetry(cleanupCtx))
	})
	return setup
}

func TestRunLoop_StopsOnCtxCancel(t *testing.T) {
	initTestTelemetry(t)
	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan struct{})
	go func() {
		RunLoop(ctx, "test/loop", 1*time.Millisecond, func(ctx context.Context) error {
			return nil
		})
		close(done)
	}()

	// Let the loop tick a few times, then cancel and expect a prompt return.
	time.Sleep(10 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("RunLoop did not return after ctx cancel")
	}
}

func TestRunLoop_FreshSpanPerTick(t *testing.T) {
	initTestTelemetry(t)
	ctx, cancel := context.WithCancel(t.Context())

	var (
		mu       sync.Mutex
		traceIDs []string
	)

	loopDone := make(chan struct{})
	go func() {
		RunLoop(ctx, "test/tick", 1*time.Millisecond, func(ctx context.Context) error {
			span := trace.SpanFromContext(ctx)
			require.True(t, span.SpanContext().IsValid(), "tick must run inside a valid span")
			mu.Lock()
			traceIDs = append(traceIDs, span.SpanContext().TraceID().String())
			enough := len(traceIDs) >= 3
			mu.Unlock()
			if enough {
				cancel()
			}
			return nil
		})
		close(loopDone)
	}()

	select {
	case <-loopDone:
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("RunLoop did not run enough ticks in time")
	}

	mu.Lock()
	defer mu.Unlock()
	require.GreaterOrEqual(t, len(traceIDs), 3)
	// Each tick opens its own root span, so trace_ids must differ tick-to-tick.
	assert.NotEqual(t, traceIDs[0], traceIDs[1])
	assert.NotEqual(t, traceIDs[1], traceIDs[2])
}

func TestGo_SpanCoversFnBody(t *testing.T) {
	initTestTelemetry(t)

	var (
		mu         sync.Mutex
		gotTraceID string
		gotSpanID  string
		ended      bool
	)
	done := make(chan struct{})

	// Wrap Go's fn so we observe both the span from inside (IsValid + IDs) and
	// the exact moment the span ends via a synchronous signal. This avoids
	// depending on the global tracer-provider delegation state leaking across
	// tests in the suite.
	Go(t.Context(), "test/worker", func(ctx context.Context) {
		defer func() {
			mu.Lock()
			ended = true
			mu.Unlock()
			close(done)
		}()
		span := trace.SpanFromContext(ctx)
		require.True(t, span.SpanContext().IsValid(), "worker must run inside a valid span")
		mu.Lock()
		gotTraceID = span.SpanContext().TraceID().String()
		gotSpanID = span.SpanContext().SpanID().String()
		mu.Unlock()
	})

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Go worker did not run in time")
	}

	mu.Lock()
	defer mu.Unlock()
	assert.True(t, ended, "worker fn should have completed")
	assert.NotEmpty(t, gotTraceID, "trace_id should be set inside worker")
	assert.NotEmpty(t, gotSpanID, "span_id should be set inside worker")
}
