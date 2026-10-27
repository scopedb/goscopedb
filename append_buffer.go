/*
 * Copyright 2024 ScopeDB, Inc.
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

package scopedb

import (
	"context"
	"sync"
)

type appendByteBudget struct {
	mu       sync.Mutex
	capacity int
	used     int
	closed   bool
	waiters  []*appendBudgetWaiter
}

func newAppendByteBudget(capacity int) *appendByteBudget {
	return &appendByteBudget{capacity: capacity}
}

type appendBudgetWaiter struct {
	bytes   int
	ready   chan struct{}
	granted bool
}

func (b *appendByteBudget) acquire(ctx context.Context, bytes int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrAppendStreamClosed
	}
	if len(b.waiters) == 0 && b.used <= b.capacity-bytes {
		b.used += bytes
		b.mu.Unlock()
		return nil
	}
	waiter := &appendBudgetWaiter{bytes: bytes, ready: make(chan struct{})}
	b.waiters = append(b.waiters, waiter)
	b.mu.Unlock()

	select {
	case <-waiter.ready:
		if waiter.granted {
			return nil
		}
		return ErrAppendStreamClosed
	case <-ctx.Done():
		b.mu.Lock()
		if waiter.granted {
			b.used -= bytes
			b.drainWaitersLocked()
		} else {
			b.removeWaiterLocked(waiter)
			b.drainWaitersLocked()
		}
		b.mu.Unlock()
		return ctx.Err()
	}
}

func (b *appendByteBudget) tryAcquire(bytes int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || len(b.waiters) > 0 || b.used > b.capacity-bytes {
		return false
	}
	b.used += bytes
	return true
}

func (b *appendByteBudget) release(bytes int) {
	b.mu.Lock()
	b.used -= bytes
	if b.used < 0 {
		b.used = 0
	}
	b.drainWaitersLocked()
	b.mu.Unlock()
}

func (b *appendByteBudget) close() {
	b.mu.Lock()
	if !b.closed {
		b.closed = true
		for _, waiter := range b.waiters {
			close(waiter.ready)
		}
		b.waiters = nil
	}
	b.mu.Unlock()
}

func (b *appendByteBudget) drainWaitersLocked() {
	for len(b.waiters) > 0 {
		waiter := b.waiters[0]
		if b.used > b.capacity-waiter.bytes {
			return
		}
		b.waiters[0] = nil
		b.waiters = b.waiters[1:]
		b.used += waiter.bytes
		waiter.granted = true
		close(waiter.ready)
	}
}

func (b *appendByteBudget) removeWaiterLocked(target *appendBudgetWaiter) {
	for index, waiter := range b.waiters {
		if waiter != target {
			continue
		}
		copy(b.waiters[index:], b.waiters[index+1:])
		b.waiters[len(b.waiters)-1] = nil
		b.waiters = b.waiters[:len(b.waiters)-1]
		return
	}
}
