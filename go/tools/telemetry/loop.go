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
	"log/slog"
	"time"
)

// RunLoop runs fn on a ticker until ctx is done, wrapping every tick in a
// fresh span named name. The span-scoped context is passed to fn so any
// context-aware logs it emits carry the tick's trace_id / span_id.
//
// fn errors are recorded on the span (via WithSpan) and logged; RunLoop does
// not stop or back off on error — the existing background loops handle their
// own error policy, and this helper deliberately does not change it.
func RunLoop(ctx context.Context, name string, interval time.Duration, fn func(context.Context) error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := WithSpan(ctx, name, fn); err != nil {
				slog.ErrorContext(ctx, "background loop tick failed", "loop", name, "error", err)
			}
		}
	}
}

// Go launches fn in a new goroutine wrapped in a span named name that lives
// for the entire call. Use it for long-running background workers that aren't
// tick-driven so their context-aware logs share a trace_id / span_id.
func Go(ctx context.Context, name string, fn func(context.Context)) {
	go func() {
		_ = WithSpan(ctx, name, func(ctx context.Context) error {
			fn(ctx)
			return nil
		})
	}()
}
