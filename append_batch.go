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
	"time"
)

type appendCommand struct {
	record  *appendRecord
	barrier *appendBarrier
}

type appendRecord struct {
	payload       []byte
	reservedBytes int
}

type appendBarrier struct {
	shutdown     bool
	startedAt    time.Time
	acceptedRows uint64
	droppedRows  uint64
	ack          chan appendBarrierResult
}

type appendBarrierResult struct {
	report AppendDeliveryReport
	err    error
}

type appendBatch struct {
	payload       []byte
	rows          int
	reservedBytes int
}

type appendBatchResult struct {
	batch appendBatch
	err   error
}

type appendWorker struct {
	stream *AppendStream

	current        []appendRecord
	currentBytes   int
	ready          []appendBatch
	inFlight       int
	barrier        *appendBarrier
	failureBarrier *appendBarrier
	baseline       appendCounters
	stopping       bool
	fatal          error
	timer          *time.Timer
	timerC         <-chan time.Time
}

func (w *appendWorker) run() {
	for {
		w.dispatchReady()
		if w.stopping {
			if w.failureBarrier != nil {
				if w.inFlight == 0 {
					if w.finishBarrier() {
						return
					}
					continue
				}
			} else {
				w.drainAfterFailure()
				if w.failureBarrier != nil {
					continue
				}
			}
			if w.inFlight == 0 {
				w.stream.finishTerminal(w.makeReport(time.Now(), w.snapshotCounters()), w.fatal, AppendStreamFailed)
				return
			}
		}
		if w.barrier != nil && len(w.ready) == 0 && w.inFlight == 0 {
			if w.finishBarrier() {
				return
			}
			continue
		}

		var commandC <-chan appendCommand
		if (!w.stopping && w.barrier == nil) || (w.stopping && w.failureBarrier == nil) {
			commandC = w.stream.commands
		}
		select {
		case command := <-commandC:
			<-w.stream.commandSlots
			w.handleCommand(command)
		case result := <-w.stream.results:
			w.handleResult(result)
		case <-w.timerC:
			w.timerC = nil
			w.finalizeCurrent()
		}
	}
}

func (w *appendWorker) handleCommand(command appendCommand) {
	if command.record != nil {
		if w.stopping {
			w.failUnsent(1, command.record.reservedBytes)
			return
		}
		w.addRecord(*command.record)
		return
	}
	if command.barrier == nil {
		return
	}
	if w.stopping {
		w.failureBarrier = command.barrier
		return
	}
	w.finalizeCurrent()
	w.barrier = command.barrier
}

func (w *appendWorker) addRecord(record appendRecord) {
	additional := len(record.payload)
	if len(w.current) > 0 {
		additional++
	}
	if len(w.current) > 0 && (w.currentBytes+additional > w.stream.config.targetBatchBytes || len(w.current) >= w.stream.config.maxBatchRows) {
		w.finalizeCurrent()
		additional = len(record.payload)
	}
	if len(w.current) == 0 {
		w.startTimer()
	}
	w.current = append(w.current, record)
	w.currentBytes += additional
	if w.currentBytes >= w.stream.config.targetBatchBytes || len(w.current) >= w.stream.config.maxBatchRows {
		w.finalizeCurrent()
	}
}

func (w *appendWorker) startTimer() {
	if w.timer == nil {
		w.timer = time.NewTimer(w.stream.config.flushInterval)
	} else {
		if !w.timer.Stop() {
			select {
			case <-w.timer.C:
			default:
			}
		}
		w.timer.Reset(w.stream.config.flushInterval)
	}
	w.timerC = w.timer.C
}

func (w *appendWorker) stopTimer() {
	if w.timer != nil && !w.timer.Stop() {
		select {
		case <-w.timer.C:
		default:
		}
	}
	w.timerC = nil
}

func (w *appendWorker) finalizeCurrent() {
	if len(w.current) == 0 {
		return
	}
	w.stopTimer()
	payload := make([]byte, 0, w.currentBytes)
	reservedBytes := 0
	for index := range w.current {
		if index > 0 {
			payload = append(payload, '\n')
		}
		payload = append(payload, w.current[index].payload...)
		reservedBytes += w.current[index].reservedBytes
	}
	w.ready = append(w.ready, appendBatch{
		payload:       payload,
		rows:          len(w.current),
		reservedBytes: reservedBytes,
	})
	w.current = nil
	w.currentBytes = 0
}

func (w *appendWorker) dispatchReady() {
	for !w.stopping && w.inFlight < w.stream.config.maxConcurrentBatches && len(w.ready) > 0 {
		batch := w.ready[0]
		w.ready[0] = appendBatch{}
		w.ready = w.ready[1:]
		w.inFlight++
		w.stream.shared.mu.Lock()
		w.stream.shared.inFlightBatches++
		w.stream.shared.mu.Unlock()
		go w.stream.sendBatch(batch)
	}
}

func (w *appendWorker) handleResult(result appendBatchResult) {
	w.inFlight--
	// Update pending counters before capacity can admit another producer.
	defer w.stream.budget.release(result.batch.reservedBytes)
	rows := uint64(result.batch.rows) // #nosec G115 -- batch rows are capped at maxAppendRows.
	w.stream.shared.mu.Lock()
	w.stream.shared.inFlightBatches--
	w.stream.shared.pendingRows -= rows
	w.stream.shared.pendingBytes -= result.batch.reservedBytes
	if result.err == nil {
		w.stream.shared.counters.committedRows += rows
		w.stream.shared.counters.committedBatches++
		w.stream.shared.mu.Unlock()
		return
	}
	state := appendErrorState(result.err)
	if state == AppendStateRejected {
		w.stream.shared.counters.failedRows += rows
		w.stream.shared.counters.failedBatches++
	} else {
		state = AppendStateUnknown
		w.stream.shared.counters.unknownRows += rows
		w.stream.shared.counters.unknownBatches++
	}
	w.stream.shared.lastFailure = newAppendLastFailure(result.err, state)
	w.stream.shared.mu.Unlock()

	if w.stream.config.failurePolicy == AppendFailureStop {
		w.startStopping(result.err)
	}
}

func (w *appendWorker) startStopping(err error) {
	if !w.stopping {
		w.stopping = true
		close(w.stream.failureDone)
		w.fatal = err
		w.stopTimer()
		w.stream.admissionMu.Lock()
		w.stream.shared.mu.Lock()
		w.stream.shared.state = AppendStreamFailed
		w.stream.shared.fatal = err
		w.stream.shared.mu.Unlock()
		w.stream.closeAdmission()
		w.stream.admissionMu.Unlock()
		for _, record := range w.current {
			w.failUnsent(1, record.reservedBytes)
		}
		w.current = nil
		w.currentBytes = 0
		for _, batch := range w.ready {
			w.failUnsent(batch.rows, batch.reservedBytes)
		}
		w.ready = nil
		if w.barrier != nil {
			w.failureBarrier = w.barrier
			w.barrier = nil
		}
		return
	}
	if appendErrorState(err) == AppendStateUnknown && appendErrorState(w.fatal) != AppendStateUnknown {
		w.fatal = err
		w.stream.shared.mu.Lock()
		w.stream.shared.fatal = err
		w.stream.shared.mu.Unlock()
	}
}

func (w *appendWorker) failUnsent(rows int, reservedBytes int) {
	count := uint64(rows) // #nosec G115 -- records and batches are capped at maxAppendRows.
	w.stream.shared.mu.Lock()
	w.stream.shared.pendingRows -= count
	w.stream.shared.pendingBytes -= reservedBytes
	w.stream.shared.counters.failedRows += count
	w.stream.shared.mu.Unlock()
	w.stream.budget.release(reservedBytes)
}

func (w *appendWorker) drainAfterFailure() {
	for {
		select {
		case command := <-w.stream.commands:
			<-w.stream.commandSlots
			w.handleCommand(command)
			if w.failureBarrier != nil {
				return
			}
		default:
			return
		}
	}
}

func (w *appendWorker) finishBarrier() bool {
	barrier := w.barrier
	var err error
	state := AppendStreamClosed
	if w.stopping {
		barrier, err, state = w.failureBarrier, w.fatal, AppendStreamFailed
		w.failureBarrier = nil
	} else {
		w.barrier = nil
	}
	counters := w.snapshotCounters()
	counters.acceptedRows = barrier.acceptedRows
	counters.droppedRows = barrier.droppedRows
	report := w.makeReport(barrier.startedAt, counters)
	w.baseline = counters
	w.stream.shared.mu.Lock()
	w.stream.shared.lastReport = &report
	w.stream.shared.mu.Unlock()
	barrier.ack <- appendBarrierResult{report: report, err: err}
	if barrier.shutdown {
		w.stream.finishTerminal(report, err, state)
		return true
	}
	return false
}

func (w *appendWorker) snapshotCounters() appendCounters {
	w.stream.shared.mu.Lock()
	defer w.stream.shared.mu.Unlock()
	return w.stream.shared.counters
}

func (w *appendWorker) makeReport(startedAt time.Time, counters appendCounters) AppendDeliveryReport {
	report := AppendDeliveryReport{
		AcceptedRows:     counters.acceptedRows - w.baseline.acceptedRows,
		CommittedRows:    counters.committedRows - w.baseline.committedRows,
		FailedRows:       counters.failedRows - w.baseline.failedRows,
		UnknownRows:      counters.unknownRows - w.baseline.unknownRows,
		DroppedRows:      counters.droppedRows - w.baseline.droppedRows,
		CommittedBatches: counters.committedBatches - w.baseline.committedBatches,
		FailedBatches:    counters.failedBatches - w.baseline.failedBatches,
		UnknownBatches:   counters.unknownBatches - w.baseline.unknownBatches,
		Retries:          counters.retries - w.baseline.retries,
		Duration:         time.Since(startedAt),
	}
	lost := report.FailedRows + report.UnknownRows + report.DroppedRows
	switch {
	case lost == 0:
		report.Outcome = AppendDeliveryOK
	case report.CommittedRows > 0:
		report.Outcome = AppendDeliveryPartial
	case report.UnknownRows > 0:
		report.Outcome = AppendDeliveryUnknown
	default:
		report.Outcome = AppendDeliveryFailed
	}
	return report
}
