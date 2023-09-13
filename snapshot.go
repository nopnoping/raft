// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"fmt"
	"io"
	"time"

	"github.com/armon/go-metrics"
)

// SnapshotMeta is for metadata of a snapshot.
// lyf: snapShot的元数据定义
type SnapshotMeta struct {
	// Version is the version number of the snapshot metadata. This does not cover
	// the application's data in the snapshot, that should be versioned
	// separately.
	// lyf: 不同版本的snapShoot；目前只有v1和v2
	Version SnapshotVersion

	// ID is opaque to the store, and is used for opening.
	// lyf: 用来读取快照数据
	ID string

	// Index and Term store when the snapshot was taken.
	// lyf: 提交snapShot时对应的index和term
	Index uint64
	Term  uint64

	// Peers is deprecated and used to support version 0 snapshots, but will
	// be populated in version 1 snapshots as well to help with upgrades.
	Peers []byte

	// Configuration and ConfigurationIndex are present in version 1
	// snapshots and later.
	// lyf: 相关的配置
	Configuration      Configuration
	ConfigurationIndex uint64

	// Size is the size of the snapshot in bytes.
	// lyf: 数据大小；但是数据存储在哪儿？
	// lyf: 该结构体仅仅只是元数据；即标记快照在磁盘上存储的关键信息；真正要读取快照数据时，是要通过ID去获取的
	// lyf: 猜测会将快照作为文件存储？ID就是文件名
	Size int64
}

// SnapshotStore interface is used to allow for flexible implementations
// of snapshot storage and retrieval. For example, a client could implement
// a shared state store such as S3, allowing new nodes to restore snapshots
// without streaming from the leader.
// lyf: snapshotStore接口定义了snapShot的存储和获取行为，可以由client根据自己的需求来实现
type SnapshotStore interface {
	// Create is used to begin a snapshot at a given index and term, and with
	// the given committed configuration. The version parameter controls
	// which snapshot version to create.
	// lyf: 创建一个snapshot，用所提供的index、term、configuration
	Create(version SnapshotVersion, index, term uint64, configuration Configuration,
		configurationIndex uint64, trans Transport) (SnapshotSink, error)

	// List is used to list the available snapshots in the store.
	// It should return then in descending order, with the highest index first.
	// lyf: 列出当前存储的所有snapshot
	List() ([]*SnapshotMeta, error)

	// Open takes a snapshot ID and provides a ReadCloser. Once close is
	// called it is assumed the snapshot is no longer needed.
	// lyf: 打开一个snapshot，会提供一个ReadCloser，当它关闭了，就认为该snapshot不需要了
	Open(id string) (*SnapshotMeta, io.ReadCloser, error)
}

// SnapshotSink is returned by StartSnapshot. The FSM will Write state
// to the sink and call Close on completion. On error, Cancel will be invoked.
// lyf: snapshotSink提供了state写的功能
type SnapshotSink interface {
	io.WriteCloser
	ID() string
	Cancel() error
}

// runSnapshots is a long running goroutine used to manage taking
// new snapshots of the FSM. It runs in parallel to the FSM and
// main goroutines, so that snapshots do not block normal operation.
// lyf: 执行snapshot的管理协程
func (r *Raft) runSnapshots() {
	for {
		select {
		// lyf: 定期snapshot
		case <-randomTimeout(r.config().SnapshotInterval):
			// Check if we should snapshot
			// lyf: 检查是否可以进行snapshot
			if !r.shouldSnapshot() {
				continue
			}

			// Trigger a snapshot
			if _, err := r.takeSnapshot(); err != nil {
				r.logger.Error("failed to take snapshot", "error", err)
			}

		// lyf: 用户触发snapshot
		case future := <-r.userSnapshotCh:
			// User-triggered, run immediately
			id, err := r.takeSnapshot()
			if err != nil {
				r.logger.Error("failed to take snapshot", "error", err)
			} else {
				future.opener = func() (*SnapshotMeta, io.ReadCloser, error) {
					return r.snapshots.Open(id)
				}
			}
			future.respond(err)

		case <-r.shutdownCh:
			return
		}
	}
}

// shouldSnapshot checks if we meet the conditions to take
// a new snapshot.
// lyf: 检查当前是否需要进行snapshot
func (r *Raft) shouldSnapshot() bool {
	// Check the last snapshot index
	lastSnap, _ := r.getLastSnapshot()

	// Check the last log index
	lastIdx, err := r.logs.LastIndex()
	if err != nil {
		r.logger.Error("failed to get last log index", "error", err)
		return false
	}

	// Compare the delta to the threshold
	delta := lastIdx - lastSnap
	// lyf: 存储的log数，大于等于阈值，执行snapshot
	return delta >= r.config().SnapshotThreshold
}

// takeSnapshot is used to take a new snapshot. This must only be called from
// the snapshot thread, never the main thread. This returns the ID of the new
// snapshot, along with an error.
// lyf: 执行snapshot，并返回这个新snapshot的id
func (r *Raft) takeSnapshot() (string, error) {
	defer metrics.MeasureSince([]string{"raft", "snapshot", "takeSnapshot"}, time.Now())

	// Create a request for the FSM to perform a snapshot.
	snapReq := &reqSnapshotFuture{}
	snapReq.init()

	// Wait for dispatch or shutdown.
	// lyf: 和fsm协程交互，获取fsmSnapshot
	select {
	case r.fsmSnapshotCh <- snapReq:
	case <-r.shutdownCh:
		return "", ErrRaftShutdown
	}

	// Wait until we get a response
	if err := snapReq.Error(); err != nil {
		if err != ErrNothingNewToSnapshot {
			err = fmt.Errorf("failed to start snapshot: %v", err)
		}
		return "", err
	}
	// lyf: 使用defer释放fsmSnapshot对象
	defer snapReq.snapshot.Release()

	// Make a request for the configurations and extract the committed info.
	// We have to use the future here to safely get this information since
	// it is owned by the main thread.
	// lyf: 为什么这里要设计为从main线程获取配置信息，而不是直接通过锁来读取？？
	configReq := &configurationsFuture{}
	configReq.ShutdownCh = r.shutdownCh
	configReq.init()
	select {
	case r.configurationsCh <- configReq:
	case <-r.shutdownCh:
		return "", ErrRaftShutdown
	}
	if err := configReq.Error(); err != nil {
		return "", err
	}
	committed := configReq.configurations.committed
	committedIndex := configReq.configurations.committedIndex

	// We don't support snapshots while there's a config change outstanding
	// since the snapshot doesn't have a means to represent this state. This
	// is a little weird because we need the FSM to apply an index that's
	// past the configuration change, even though the FSM itself doesn't see
	// the configuration changes. It should be ok in practice with normal
	// application traffic flowing through the FSM. If there's none of that
	// then it's not crucial that we snapshot, since there's not much going
	// on Raft-wise.
	// lyf: 和配置里的commitIndex比较？是干啥？没看懂？
	if snapReq.index < committedIndex {
		return "", fmt.Errorf("cannot take snapshot now, wait until the configuration entry at %v has been applied (have applied %v)",
			committedIndex, snapReq.index)
	}

	// Create a new snapshot.
	// lyf: 创建具体的snapshot；通过snapshot存储引擎
	r.logger.Info("starting snapshot up to", "index", snapReq.index)
	start := time.Now()
	version := getSnapshotVersion(r.protocolVersion)
	// lyf: 创建snapShotSink，这样就可以调用FSMSnapshot的persist
	sink, err := r.snapshots.Create(version, snapReq.index, snapReq.term, committed, committedIndex, r.trans)
	if err != nil {
		return "", fmt.Errorf("failed to create snapshot: %v", err)
	}
	metrics.MeasureSince([]string{"raft", "snapshot", "create"}, start)

	// Try to persist the snapshot.
	// lyf: 持久化
	start = time.Now()
	if err := snapReq.snapshot.Persist(sink); err != nil {
		sink.Cancel()
		return "", fmt.Errorf("failed to persist snapshot: %v", err)
	}
	metrics.MeasureSince([]string{"raft", "snapshot", "persist"}, start)

	// Close and check for error.
	// lyf: 关闭sink提供的io.write
	if err := sink.Close(); err != nil {
		return "", fmt.Errorf("failed to close snapshot: %v", err)
	}

	// Update the last stable snapshot info.
	r.setLastSnapshot(snapReq.index, snapReq.term)

	// Compact the logs.
	// lyf: logs瘦身
	if err := r.compactLogs(snapReq.index); err != nil {
		return "", err
	}

	r.logger.Info("snapshot complete up to", "index", snapReq.index)
	return sink.ID(), nil
}

// compactLogsWithTrailing takes the last inclusive index of a snapshot,
// the lastLogIdx, and and the trailingLogs and trims the logs that
// are no longer needed.
func (r *Raft) compactLogsWithTrailing(snapIdx uint64, lastLogIdx uint64, trailingLogs uint64) error {
	// Determine log ranges to compact
	minLog, err := r.logs.FirstIndex()
	if err != nil {
		return fmt.Errorf("failed to get first log index: %v", err)
	}

	// Check if we have enough logs to truncate
	// Use a consistent value for trailingLogs for the duration of this method
	// call to avoid surprising behaviour.
	if lastLogIdx <= trailingLogs {
		return nil
	}

	// Truncate up to the end of the snapshot, or `TrailingLogs`
	// back from the head, which ever is further back. This ensures
	// at least `TrailingLogs` entries, but does not allow logs
	// after the snapshot to be removed.
	maxLog := min(snapIdx, lastLogIdx-trailingLogs)

	if minLog > maxLog {
		r.logger.Info("no logs to truncate")
		return nil
	}

	r.logger.Info("compacting logs", "from", minLog, "to", maxLog)

	// Compact the logs
	if err := r.logs.DeleteRange(minLog, maxLog); err != nil {
		return fmt.Errorf("log compaction failed: %v", err)
	}
	return nil
}

// compactLogs takes the last inclusive index of a snapshot
// and trims the logs that are no longer needed.
func (r *Raft) compactLogs(snapIdx uint64) error {
	defer metrics.MeasureSince([]string{"raft", "compactLogs"}, time.Now())

	lastLogIdx, _ := r.getLastLog()
	trailingLogs := r.config().TrailingLogs

	return r.compactLogsWithTrailing(snapIdx, lastLogIdx, trailingLogs)
}

// removeOldLogs removes all old logs from the store. This is used for
// MonotonicLogStores after restore. Callers should verify that the store
// implementation is monotonic prior to calling.
func (r *Raft) removeOldLogs() error {
	defer metrics.MeasureSince([]string{"raft", "removeOldLogs"}, time.Now())

	lastLogIdx, err := r.logs.LastIndex()
	if err != nil {
		return fmt.Errorf("failed to get last log index: %w", err)
	}

	r.logger.Info("removing all old logs from log store")

	// call compactLogsWithTrailing with lastLogIdx for snapIdx since
	// it will take the lesser of lastLogIdx and snapIdx to figure out
	// the end for which to apply trailingLogs.
	return r.compactLogsWithTrailing(lastLogIdx, lastLogIdx, 0)
}
