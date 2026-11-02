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
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"slices"
	"time"
)

func (s *AppendStream) sendBatch(batch appendBatch) {
	var err error
	backoff := s.config.retry.InitialBackoff
	deliveryCtx, deliveryCancel := context.WithTimeout(context.Background(), s.config.retry.MaxElapsedTime)
	defer deliveryCancel()
	hadAmbiguous := false
retryLoop:
	for attempt := 0; ; attempt++ {
		if attempt > 0 {
			if deliveryCtx.Err() != nil {
				break
			}
			select {
			case <-s.failureDone:
				break retryLoop
			default:
			}
			s.shared.mu.Lock()
			s.shared.counters.retries++
			s.shared.mu.Unlock()
		}
		_, err = s.client.appendNDJSON(
			deliveryCtx,
			s.database,
			s.schema,
			s.table,
			batch.payload,
			s.config.attemptTimeout,
		)
		// In-flight failures always carry AppendDetails. Errors without them
		// happened before dispatch (for example, the retry budget expired while
		// compressing). Those rows were not sent by this attempt.
		if err != nil {
			if failure, ok := errors.AsType[*Error](err); !ok || failure.AppendDetails == nil {
				rejected := appendDefiniteFailure(err.Error())
				rejected.cause = err
				err = rejected
			}
		}
		if err != nil && appendErrorOutcome(err) == AppendOutcomeAmbiguous {
			hadAmbiguous = true
		}
		if err == nil || !s.shouldRetryAppend(err) {
			break
		}
		if attempt >= s.config.retry.MaxRetries {
			err = fmt.Errorf("%w after %d attempts: %w", ErrAppendRetryExhausted, attempt+1, err)
			break
		}
		var retryAfter time.Duration
		if scopeErr, ok := errors.AsType[*Error](err); ok {
			retryAfter = scopeErr.RetryAfter
		}
		delay := appendRetryDelay(backoff, retryAfter)
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-deliveryCtx.Done():
			timer.Stop()
			break retryLoop
		case <-s.failureDone:
			timer.Stop()
			break retryLoop
		}
		if backoff > s.config.retry.MaxBackoff/2 {
			backoff = s.config.retry.MaxBackoff
		} else {
			backoff *= 2
		}
	}
	if err != nil && deliveryCtx.Err() != nil {
		err = fmt.Errorf("%w after elapsed budget %s: %w", ErrAppendRetryExhausted, s.config.retry.MaxElapsedTime, err)
	}
	// A later rejected attempt cannot prove an earlier unknown attempt did not
	// commit. Only a successful acknowledgement resolves the batch for replay.
	if err != nil && hadAmbiguous && appendErrorOutcome(err) != AppendOutcomeAmbiguous {
		err = ambiguousAppendStreamError("an earlier append attempt may have committed; last attempt: "+err.Error(), err)
	}
	s.results <- appendBatchResult{batch: batch, err: err}
}

func appendRetryDelay(backoff time.Duration, retryAfter time.Duration) time.Duration {
	// Equal jitter avoids synchronized retries. Retry-After is a lower bound,
	// even when longer than MaxBackoff; the elapsed budget bounds the wait.
	delay := backoff/2 + time.Duration(rand.Int64N(max(int64(backoff-backoff/2), 1))) // #nosec G404 -- retry jitter does not require cryptographic randomness.
	if retryAfter > delay {
		delay = retryAfter
	}
	return delay
}

func (s *AppendStream) shouldRetryAppend(err error) bool {
	failure, ok := errors.AsType[*Error](err)
	if !ok || failure.AppendDetails == nil {
		return false
	}
	if failure.ServerRetryable != nil && !*failure.ServerRetryable {
		return false
	}
	if failure.AppendDetails.Status == AppendOutcomeFailed {
		return failure.Retryable
	}
	if !s.config.retry.RetryAmbiguous || failure.AppendDetails.Status != AppendOutcomeAmbiguous {
		return false
	}
	if errors.Is(err, context.Canceled) || failure.WriteDetails != nil {
		return false
	}
	// Authentication, schema, and other permanent HTTP errors must not spin.
	return failure.HTTPStatus == 0 || (failure.HTTPStatus >= 200 && failure.HTTPStatus < 300) ||
		failure.HTTPStatus == http.StatusRequestTimeout || failure.HTTPStatus == http.StatusTooManyRequests ||
		failure.HTTPStatus >= 500
}

func ambiguousAppendStreamError(message string, cause error) error {
	result := &Error{
		Kind:      ErrorKindAppendRowsFailed,
		Message:   message,
		Retryable: false,
		AppendDetails: &AppendErrorDetails{
			Status:    AppendOutcomeAmbiguous,
			RowErrors: []AppendRowError{},
		},
		cause: cause,
	}
	// Preserve the last attempt's diagnostic metadata while the state describes
	// the outcome across all attempts, including an earlier lost response.
	if last, ok := errors.AsType[*Error](cause); ok {
		result.Code = last.Code
		result.ServerRetryable = last.ServerRetryable
		result.Details = slices.Clone(last.Details)
		result.WriteDetails = last.WriteDetails
		result.HTTPStatus = last.HTTPStatus
		result.RequestID = last.RequestID
		result.RetryAfter = last.RetryAfter
		if last.AppendDetails != nil {
			result.AppendDetails.RowErrors = slices.Clone(last.AppendDetails.RowErrors)
			result.AppendDetails.RowErrorsTruncated = last.AppendDetails.RowErrorsTruncated
		}
	}
	return result
}

func appendErrorOutcome(err error) AppendOutcome {
	if scopeErr, ok := errors.AsType[*Error](err); ok && scopeErr.AppendDetails != nil {
		return scopeErr.AppendDetails.Status
	}
	return AppendOutcomeAmbiguous
}

func newAppendLastFailure(err error, state AppendOutcome) *AppendLastFailure {
	failure := &AppendLastFailure{
		At:      time.Now(),
		Message: err.Error(),
		Status:  state,
	}
	scopeErr, ok := errors.AsType[*Error](err)
	if !ok {
		return failure
	}
	failure.Code = scopeErr.Code
	failure.Details = slices.Clone(scopeErr.Details)
	failure.HTTPStatus = scopeErr.HTTPStatus
	failure.RequestID = scopeErr.RequestID
	failure.RetryAfter = scopeErr.RetryAfter
	failure.Retryable = scopeErr.Retryable
	if scopeErr.WriteDetails != nil {
		details := *scopeErr.WriteDetails
		failure.WriteDetails = &details
	}
	if scopeErr.AppendDetails != nil {
		failure.RowErrors = slices.Clone(scopeErr.AppendDetails.RowErrors)
		failure.RowErrorsTruncated = scopeErr.AppendDetails.RowErrorsTruncated
	}
	return failure
}

func cloneAppendLastFailure(failure *AppendLastFailure) *AppendLastFailure {
	if failure == nil {
		return nil
	}
	cloned := *failure
	cloned.Details = slices.Clone(failure.Details)
	cloned.RowErrors = slices.Clone(failure.RowErrors)
	if failure.WriteDetails != nil {
		details := *failure.WriteDetails
		cloned.WriteDetails = &details
	}
	return &cloned
}
