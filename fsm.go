// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"fmt"
	"io"
	"time"

	"github.com/armon/go-metrics"
	hclog "github.com/hashicorp/go-hclog"
)

// FSM is implemented by clients to make use of the replicated log.
// lyf: 由具体的客户端来实现如何处理已经commit的log
type FSM interface {
	// Apply is called once a log entry is committed by a majority of the cluster.
	//
	// Apply should apply the log to the FSM. Apply must be deterministic and
	// produce the same result on all peers in the cluster.
	//
	// The returned value is returned to the client as the ApplyFuture.Response.
	// lyf: 执行commit log
	Apply(*Log) interface{}

	// Snapshot returns an FSMSnapshot used to: support log compaction, to
	// restore the FSM to a previous state, or to bring out-of-date followers up
	// to a recent log index.
	//
	// The Snapshot implementation should return quickly, because Apply can not
	// be called while Snapshot is running. Generally this means Snapshot should
	// only capture a pointer to the state, and any expensive IO should happen
	// as part of FSMSnapshot.Persist.
	//
	// Apply and Snapshot are always called from the same thread, but Apply will
	// be called concurrently with FSMSnapshot.Persist. This means the FSM should
	// be implemented to allow for concurrent updates while a snapshot is happening.
	// lyf: 获取快照
	// lyf: 该方法要快速返回，因为apply和snapshot是在同一个协程中处理的，所以snapshot会阻塞apply
	// lyf: 因此snapshot被设计为快速返回一个fsmSnapshot的方法，昂贵的io操作由fsmSnapshot执行
	// lyf：apply和fsmSnapshot.Persist是并发的，因此fsm需要被设计为允许在snapshot时，依然在更新数据！！
	Snapshot() (FSMSnapshot, error)

	// Restore is used to restore an FSM from a snapshot. It is not called
	// concurrently with any other command. The FSM must discard all previous
	// state before restoring the snapshot.
	// lyf: 使用快照进行恢复
	Restore(snapshot io.ReadCloser) error
}

// BatchingFSM extends the FSM interface to add an ApplyBatch function. This can
// optionally be implemented by clients to enable multiple logs to be applied to
// the FSM in batches. Up to MaxAppendEntries could be sent in a batch.
type BatchingFSM interface {
	// ApplyBatch is invoked once a batch of log entries has been committed and
	// are ready to be applied to the FSM. ApplyBatch will take in an array of
	// log entries. These log entries will be in the order they were committed,
	// will not have gaps, and could be of a few log types. Clients should check
	// the log type prior to attempting to decode the data attached. Presently
	// the LogCommand and LogConfiguration types will be sent.
	//
	// The returned slice must be the same length as the input and each response
	// should correlate to the log at the same index of the input. The returned
	// values will be made available in the ApplyFuture returned by Raft.Apply
	// method if that method was called on the same Raft node as the FSM.
	ApplyBatch([]*Log) []interface{}

	FSM
}

// FSMSnapshot is returned by an FSM in response to a Snapshot
// It must be safe to invoke FSMSnapshot methods with concurrent
// calls to Apply.
type FSMSnapshot interface {
	// Persist should dump all necessary state to the WriteCloser 'sink',
	// and call sink.Close() when finished or call sink.Cancel() on error.
	Persist(sink SnapshotSink) error

	// Release is invoked when we are finished with the snapshot.
	Release()
}

// runFSM is a long running goroutine responsible for applying logs
// to the FSM. This is done async of other logs since we don't want
// the FSM to block our internal operations.
func (r *Raft) runFSM() {
	var lastIndex, lastTerm uint64

	// lyf: 通过解析指针的方式，看是否client实现的是否支持该接口
	batchingFSM, batchingEnabled := r.fsm.(BatchingFSM)
	configStore, configStoreEnabled := r.fsm.(ConfigurationStore)

	applySingle := func(req *commitTuple) {
		// Apply the log if a command or config change
		var resp interface{}
		// Make sure we send a response
		// lyf: 用defer来给future回复
		// lyf: 这里defer后的是一个func，可以执行多个步骤
		defer func() {
			// Invoke the future if given
			if req.future != nil {
				req.future.response = resp
				req.future.respond(nil)
			}
		}()

		switch req.log.Type {
		case LogCommand:
			start := time.Now()
			// lyf: 调用FSM的实现，执行log
			resp = r.fsm.Apply(req.log)
			metrics.MeasureSince([]string{"raft", "fsm", "apply"}, start)

		case LogConfiguration:
			if !configStoreEnabled {
				// Return early to avoid incrementing the index and term for
				// an unimplemented operation.
				return
			}

			start := time.Now()
			configStore.StoreConfiguration(req.log.Index, DecodeConfiguration(req.log.Data))
			metrics.MeasureSince([]string{"raft", "fsm", "store_config"}, start)
		}

		// Update the indexes
		lastIndex = req.log.Index
		lastTerm = req.log.Term
	}

	// lyf: 批量的apply log
	applyBatch := func(reqs []*commitTuple) {
		if !batchingEnabled {
			// lyf: 降级为单次执行
			for _, ct := range reqs {
				applySingle(ct)
			}
			return
		}

		// Only send LogCommand and LogConfiguration log types. LogBarrier types
		// will not be sent to the FSM.
		// lyf: 判断该log是否需要执行
		shouldSend := func(l *Log) bool {
			switch l.Type {
			case LogCommand, LogConfiguration:
				return true
			}
			return false
		}

		var lastBatchIndex, lastBatchTerm uint64
		sendLogs := make([]*Log, 0, len(reqs))
		// lyf: 把需要执行的log过滤出来
		for _, req := range reqs {
			if shouldSend(req.log) {
				sendLogs = append(sendLogs, req.log)
			}
			lastBatchIndex = req.log.Index
			lastBatchTerm = req.log.Term
		}

		var responses []interface{}
		// lyf: 调用client的批量接口执行
		if len(sendLogs) > 0 {
			start := time.Now()
			responses = batchingFSM.ApplyBatch(sendLogs)
			metrics.MeasureSince([]string{"raft", "fsm", "applyBatch"}, start)
			metrics.AddSample([]string{"raft", "fsm", "applyBatchNum"}, float32(len(reqs)))

			// Ensure we get the expected responses
			if len(sendLogs) != len(responses) {
				panic("invalid number of responses")
			}
		}

		// Update the indexes
		lastIndex = lastBatchIndex
		lastTerm = lastBatchTerm

		var i int
		// lyf: 将resp传递给对应future
		for _, req := range reqs {
			var resp interface{}
			// If the log was sent to the FSM, retrieve the response.
			if shouldSend(req.log) {
				resp = responses[i]
				i++
			}

			if req.future != nil {
				req.future.response = resp
				req.future.respond(nil)
			}
		}
	}

	restore := func(req *restoreFuture) {
		// Open the snapshot
		meta, source, err := r.snapshots.Open(req.ID)
		if err != nil {
			req.respond(fmt.Errorf("failed to open snapshot %v: %v", req.ID, err))
			return
		}
		defer source.Close()

		snapLogger := r.logger.With(
			"id", req.ID,
			"last-index", meta.Index,
			"last-term", meta.Term,
			"size-in-bytes", meta.Size,
		)

		// Attempt to restore
		if err := fsmRestoreAndMeasure(snapLogger, r.fsm, source, meta.Size); err != nil {
			req.respond(fmt.Errorf("failed to restore snapshot %v: %v", req.ID, err))
			return
		}

		// Update the last index and term
		lastIndex = meta.Index
		lastTerm = meta.Term
		req.respond(nil)
	}

	// lyf: 调用fsm的Snapshot方法，快速返回
	snapshot := func(req *reqSnapshotFuture) {
		// Is there something to snapshot?
		if lastIndex == 0 {
			req.respond(ErrNothingNewToSnapshot)
			return
		}

		// Start a snapshot
		start := time.Now()
		snap, err := r.fsm.Snapshot()
		metrics.MeasureSince([]string{"raft", "fsm", "snapshot"}, start)

		// Respond to the request
		req.index = lastIndex
		req.term = lastTerm
		req.snapshot = snap
		req.respond(err)
	}

	saturation := newSaturationMetric([]string{"raft", "thread", "fsm", "saturation"}, 1*time.Second)

	// lyf: 主流程
	for {
		saturation.sleeping()

		select {
		// lyf: apply log
		case ptr := <-r.fsmMutateCh:
			saturation.working()

			// lyf: 变量的.type表示类型
			switch req := ptr.(type) {
			case []*commitTuple:
				applyBatch(req)

			case *restoreFuture:
				restore(req)

			default:
				panic(fmt.Errorf("bad type passed to fsmMutateCh: %#v", ptr))
			}

		// lyf: take snapshot
		case req := <-r.fsmSnapshotCh:
			saturation.working()

			snapshot(req)

		case <-r.shutdownCh:
			return
		}
	}
}

// fsmRestoreAndMeasure wraps the Restore call on an FSM to consistently measure
// and report timing metrics. The caller is still responsible for calling Close
// on the source in all cases.
func fsmRestoreAndMeasure(logger hclog.Logger, fsm FSM, source io.ReadCloser, snapshotSize int64) error {
	start := time.Now()

	crc := newCountingReadCloser(source)

	monitor := startSnapshotRestoreMonitor(logger, crc, snapshotSize, false)
	defer monitor.StopAndWait()

	if err := fsm.Restore(crc); err != nil {
		return err
	}
	metrics.MeasureSince([]string{"raft", "fsm", "restore"}, start)
	metrics.SetGauge([]string{"raft", "fsm", "lastRestoreDuration"},
		float32(time.Since(start).Milliseconds()))

	return nil
}
