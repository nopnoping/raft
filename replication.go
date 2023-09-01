// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/armon/go-metrics"
)

const (
	maxFailureScale = 12
	failureWait     = 10 * time.Millisecond
)

var (
	// ErrLogNotFound indicates a given log entry is not available.
	ErrLogNotFound = errors.New("log not found")

	// ErrPipelineReplicationNotSupported can be returned by the transport to
	// signal that pipeline replication is not supported in general, and that
	// no error message should be produced.
	ErrPipelineReplicationNotSupported = errors.New("pipeline replication not supported")
)

// followerReplication is in charge of sending snapshots and log entries from
// this leader during this particular term to a remote follower.
// lyf: 该结构体是一个重点结构体；用来和follower同步
type followerReplication struct {
	// currentTerm and nextIndex must be kept at the top of the struct so
	// they're 64 bit aligned which is a requirement for atomic ops on 32 bit
	// platforms.

	// currentTerm is the term of this leader, to be included in AppendEntries
	// requests.
	// lyf: leader的term
	currentTerm uint64

	// nextIndex is the index of the next log entry to send to the follower,
	// which may fall past the end of the log.
	// lyf: 该follower下一个要同步的index
	nextIndex uint64

	// peer contains the network address and ID of the remote follower.
	// lyf: follower的地址信息
	peer Server
	// peerLock protects 'peer'
	peerLock sync.RWMutex

	// commitment tracks the entries acknowledged by followers so that the
	// leader's commit index can advance. It is updated on successful
	// AppendEntries responses.
	// lyf: 指针？这个应该就是LeaderState里的commitment；用来计算commit
	commitment *commitment

	// stopCh is notified/closed when this leader steps down or the follower is
	// removed from the cluster. In the follower removed case, it carries a log
	// index; replication should be attempted with a best effort up through that
	// index, before exiting.
	// lyf: 当leader不再是leader或则follower从集群中移除时，使用该channel来通知
	// lyf: 并且会传递一个index，让follower尽量同步到这里
	stopCh chan uint64

	// triggerCh is notified every time new entries are appended to the log.
	// lyf: 当有新的日志需要同步时，就用这个触发
	triggerCh chan struct{}

	// triggerDeferErrorCh is used to provide a backchannel. By sending a
	// deferErr, the sender can be notifed when the replication is done.
	// lyf: 用来触发错误的？
	triggerDeferErrorCh chan *deferError

	// lastContact is updated to the current time whenever any response is
	// received from the follower (successful or not). This is used to check
	// whether the leader should step down (Raft.checkLeaderLease()).
	// lyf: 最后一次联系时间；维护了这个数据，可以用来优化
	lastContact time.Time
	// lastContactLock protects 'lastContact'.
	lastContactLock sync.RWMutex

	// failures counts the number of failed RPCs since the last success, which is
	// used to apply backoff.
	// lyf: 失败的rpc次数
	failures uint64

	// notifyCh is notified to send out a heartbeat, which is used to check that
	// this server is still leader.
	// lyf: 通过这个channel来触发心跳
	notifyCh chan struct{}
	// notify is a map of futures to be resolved upon receipt of an
	// acknowledgement, then cleared from this map.
	// lyf: 这个是用来干啥的，没看懂
	// lyf: 存储leader验证future
	notify map[*verifyFuture]struct{}
	// notifyLock protects 'notify'.
	notifyLock sync.Mutex

	// stepDown is used to indicate to the leader that we
	// should step down based on information from a follower.
	// lyf: 用来让leader退化的？
	stepDown chan struct{}

	// allowPipeline is used to determine when to pipeline the AppendEntries RPCs.
	// It is private to this replication goroutine.
	// lyf: 并发？
	allowPipeline bool
}

// notifyAll is used to notify all the waiting verify futures
// if the follower believes we are still the leader.
// lyf: 该方法是辅助验证当前leader还是不是leader
// lyf: 告诉verifyFuture当前follower还认不认这个leader
func (s *followerReplication) notifyAll(leader bool) {
	// Clear the waiting notifies minimizing lock time
	s.notifyLock.Lock()
	n := s.notify
	s.notify = make(map[*verifyFuture]struct{})
	s.notifyLock.Unlock()

	// Submit our votes
	for v := range n {
		v.vote(leader)
	}
}

// cleanNotify is used to delete notify, .
func (s *followerReplication) cleanNotify(v *verifyFuture) {
	s.notifyLock.Lock()
	delete(s.notify, v)
	s.notifyLock.Unlock()
}

// LastContact returns the time of last contact.
func (s *followerReplication) LastContact() time.Time {
	s.lastContactLock.RLock()
	last := s.lastContact
	s.lastContactLock.RUnlock()
	return last
}

// setLastContact sets the last contact to the current time.
func (s *followerReplication) setLastContact() {
	s.lastContactLock.Lock()
	s.lastContact = time.Now()
	s.lastContactLock.Unlock()
}

// replicate is a long running routine that replicates log entries to a single
// follower.
// lyf: 该文件是同步文件，但是实现的确实raft结构体的方法；同一个包下，任何地方都可以去实现结构体的方法
// lyf: 这样可以更好的将处理同一领域的方法，放在一起？
// lyf: 启动同步
func (r *Raft) replicate(s *followerReplication) {
	// Start an async heartbeating routing
	stopHeartbeat := make(chan struct{})
	defer close(stopHeartbeat)
	// lyf: 启动心跳协程
	r.goFunc(func() { r.heartbeat(s, stopHeartbeat) })

RPC:
	// lyf: 这个用在哪儿？
	// lyf: 同步时，如果收到了stopCh，就会返回true，此时就可以停止
	shouldStop := false
	for !shouldStop {
		select {
		// lyf: leader变更的优化；
		// lyf: 发送一个index，让follower尽量同步到这儿，然后关闭协程
		case maxIndex := <-s.stopCh:
			// Make a best effort to replicate up to this index
			if maxIndex > 0 {
				r.replicateTo(s, maxIndex)
			}
			return
		// lyf: 这个是提供的哪个场景？
		case deferErr := <-s.triggerDeferErrorCh:
			lastLogIdx, _ := r.getLastLog()
			shouldStop = r.replicateTo(s, lastLogIdx)
			if !shouldStop {
				deferErr.respond(nil)
			} else {
				deferErr.respond(fmt.Errorf("replication failed"))
			}
		// lyf: 当有新的log时，会触发
		case <-s.triggerCh:
			lastLogIdx, _ := r.getLastLog()
			shouldStop = r.replicateTo(s, lastLogIdx)
		// This is _not_ our heartbeat mechanism but is to ensure
		// followers quickly learn the leader's commit index when
		// raft commits stop flowing naturally. The actual heartbeats
		// can't do this to keep them unblocked by disk IO on the
		// follower. See https://github.com/hashicorp/raft/issues/282.
		// lyf: 心跳不会去同步数据，就不会有新的commit；
		// lyf: 有时会由于disk/network延时，导致数据没有同步上，所以会周期性的同步一下
		// lyf: 兜底？
		case <-randomTimeout(r.config().CommitTimeout):
			lastLogIdx, _ := r.getLastLog()
			shouldStop = r.replicateTo(s, lastLogIdx)
		}

		// If things looks healthy, switch to pipeline mode
		// lyf: 只要同步成功，就会切换到pipeline模式
		if !shouldStop && s.allowPipeline {
			goto PIPELINE
		}
	}
	return

PIPELINE:
	// Disable until re-enabled
	s.allowPipeline = false

	// Replicates using a pipeline for high performance. This method
	// is not able to gracefully recover from errors, and so we fall back
	// to standard mode on failure.
	// lyf: 使用pipeline进行同步；如果出错，就退化到普通的
	if err := r.pipelineReplicate(s); err != nil {
		if err != ErrPipelineReplicationNotSupported {
			s.peerLock.RLock()
			peer := s.peer
			s.peerLock.RUnlock()
			r.logger.Error("failed to start pipeline replication to", "peer", peer, "error", err)
		}
	}
	goto RPC
}

// replicateTo is a helper to replicate(), used to replicate the logs up to a
// given last index.
// If the follower log is behind, we take care to bring them up to date.
// lyf: 将follower的lastIndex同步到目标index
func (r *Raft) replicateTo(s *followerReplication, lastIndex uint64) (shouldStop bool) {
	// Create the base request
	var req AppendEntriesRequest
	var resp AppendEntriesResponse
	var start time.Time
	var peer Server

START:
	// Prevent an excessive retry rate on errors
	// lyf: 防止失败后一直重试；所以根据失败次数，计算一个延时
	if s.failures > 0 {
		select {
		case <-time.After(backoff(failureWait, s.failures, maxFailureScale)):
		case <-r.shutdownCh:
		}
	}

	s.peerLock.RLock()
	peer = s.peer
	s.peerLock.RUnlock()

	// Setup the request
	// lyf: 构造appendEntries的请求参数
	if err := r.setupAppendEntries(s, &req, atomic.LoadUint64(&s.nextIndex), lastIndex); err == ErrLogNotFound {
		// lyf: 当日志获取失败时，可能是由于该日志位于snapShoot中，所以需要进行快照同步
		goto SEND_SNAP
	} else if err != nil {
		return
	}

	// Make the RPC call
	start = time.Now()
	// lyf: 发起appendEntries
	if err := r.trans.AppendEntries(peer.ID, peer.Address, &req, &resp); err != nil {
		r.logger.Error("failed to appendEntries to", "peer", peer, "error", err)
		s.failures++
		// lyf: 这里是return，也就是说失败之后，不是循环去重试，也就导致网络原因引发同步之后；如果不周期的再次触发同步，数据就无法提交
		// lyf: 这里如果改为goto START 会有优化效果吗？
		// lyf: optimization
		return
	}
	// lyf: metrics
	appendStats(string(peer.ID), start, float32(len(req.Entries)))

	// Check for a newer term, stop running
	// lyf: follower有更大的term；当前leader需要退化
	if resp.Term > req.Term {
		r.handleStaleTerm(s)
		return true
	}

	// Update the last contact
	// lyf: 记录最后一次通信时间
	s.setLastContact()

	// Update s based on success
	if resp.Success {
		// Update our replication state
		// lyf: 更新该follower的lastIndex
		updateLastAppended(s, &req)

		// Clear any failures, allow pipelining
		// lyf: 清除失败记录；并且开启pipeline
		// lyf: 所以只要成功一次，就会开启？
		s.failures = 0
		s.allowPipeline = true
	} else {
		// lyf: 失败之后的尝试，是论文中的减一；这里可以优化？
		// lyf: optimization
		atomic.StoreUint64(&s.nextIndex, max(min(s.nextIndex-1, resp.LastLog+1), 1))
		if resp.NoRetryBackoff {
			s.failures = 0
		} else {
			s.failures++
		}
		r.logger.Warn("appendEntries rejected, sending older logs", "peer", peer, "next", atomic.LoadUint64(&s.nextIndex))
	}

CHECK_MORE:
	// Poll the stop channel here in case we are looping and have been asked
	// to stop, or have stepped down as leader. Even for the best effort case
	// where we are asked to replicate to a given index and then shutdown,
	// it's better to not loop in here to send lots of entries to a straggler
	// that's leaving the cluster anyways.
	// lyf: 为什么这里还要获取stopCh的消息？在replicate中不是已经处理个该消息吗？
	// lyf: 这里解决的场景，是正在循环的去同步，但是又要求关闭，可以快速结束
	select {
	case <-s.stopCh:
		return true
	default:
	}

	// Check if there are more logs to replicate
	// lyf: 是否同步到lastIndex，没有就继续
	if atomic.LoadUint64(&s.nextIndex) <= lastIndex {
		goto START
	}
	return

	// SEND_SNAP is used when we fail to get a log, usually because the follower
	// is too far behind, and we must ship a snapshot down instead
SEND_SNAP:
	// lyf: nextLog在快照中，进行快照同步
	if stop, err := r.sendLatestSnapshot(s); stop {
		return true
	} else if err != nil {
		r.logger.Error("failed to send snapshot to", "peer", peer, "error", err)
		return
	}

	// Check if there is more to replicate
	goto CHECK_MORE
}

// sendLatestSnapshot is used to send the latest snapshot we have
// down to our follower.
// lyf: 给follower发送快照
func (r *Raft) sendLatestSnapshot(s *followerReplication) (bool, error) {
	// Get the snapshots
	// lyf: 获取snapshot元信息
	snapshots, err := r.snapshots.List()
	if err != nil {
		r.logger.Error("failed to list snapshots", "error", err)
		return false, err
	}

	// Check we have at least a single snapshot
	if len(snapshots) == 0 {
		return false, fmt.Errorf("no snapshots found")
	}

	// Open the most recent snapshot
	// lyf: 获取最新的snapshot数据
	snapID := snapshots[0].ID
	meta, snapshot, err := r.snapshots.Open(snapID)
	if err != nil {
		r.logger.Error("failed to open snapshot", "id", snapID, "error", err)
		return false, err
	}
	defer snapshot.Close()

	// Setup the request
	// lyf: 构造req
	req := InstallSnapshotRequest{
		RPCHeader:       r.getRPCHeader(),
		SnapshotVersion: meta.Version,
		Term:            s.currentTerm,
		// this is needed for retro compatibility, before RPCHeader.Addr was added
		Leader:             r.trans.EncodePeer(r.localID, r.localAddr),
		LastLogIndex:       meta.Index,
		LastLogTerm:        meta.Term,
		Peers:              meta.Peers,
		Size:               meta.Size,
		Configuration:      EncodeConfiguration(meta.Configuration),
		ConfigurationIndex: meta.ConfigurationIndex,
	}

	s.peerLock.RLock()
	peer := s.peer
	s.peerLock.RUnlock()

	// Make the call
	start := time.Now()
	var resp InstallSnapshotResponse
	// lyf: 发送InstallSnapshot请求；数据是在传输层中传递的
	if err := r.trans.InstallSnapshot(peer.ID, peer.Address, &req, &resp, snapshot); err != nil {
		r.logger.Error("failed to install snapshot", "id", snapID, "error", err)
		s.failures++
		return false, err
	}
	labels := []metrics.Label{{Name: "peer_id", Value: string(peer.ID)}}
	metrics.MeasureSinceWithLabels([]string{"raft", "replication", "installSnapshot"}, start, labels)
	// Duplicated information. Kept for backward compatibility.
	metrics.MeasureSince([]string{"raft", "replication", "installSnapshot", string(peer.ID)}, start)

	// Check for a newer term, stop running
	// lyf: 下面这些和AppendEntries的发送类型
	if resp.Term > req.Term {
		r.handleStaleTerm(s)
		return true, nil
	}

	// Update the last contact
	s.setLastContact()

	// Check for success
	if resp.Success {
		// Update the indexes
		// lyf: 更新nextIndex，并计算commit
		atomic.StoreUint64(&s.nextIndex, meta.Index+1)
		s.commitment.match(peer.ID, meta.Index)

		// Clear any failures
		s.failures = 0

		// Notify we are still leader
		s.notifyAll(true)
	} else {
		s.failures++
		r.logger.Warn("installSnapshot rejected to", "peer", peer)
	}
	return false, nil
}

// heartbeat is used to periodically invoke AppendEntries on a peer
// to ensure they don't time out. This is done async of replicate(),
// since that routine could potentially be blocked on disk IO.
// lyf: 定期发送心跳
func (r *Raft) heartbeat(s *followerReplication, stopCh chan struct{}) {
	var failures uint64
	// lyf: 心跳不需要去验证prvLog这些吗？
	// lyf: 因为心跳并不关心返回的结果是true or false；只要能正确发送即可
	req := AppendEntriesRequest{
		RPCHeader: r.getRPCHeader(),
		Term:      s.currentTerm,
		// this is needed for retro compatibility, before RPCHeader.Addr was added
		Leader: r.trans.EncodePeer(r.localID, r.localAddr),
	}

	var resp AppendEntriesResponse
	for {
		// Wait for the next heartbeat interval or forced notify
		select {
		// lyf: notifyCh是用来强制发送心跳，而无需等待超时
		case <-s.notifyCh:
		case <-randomTimeout(r.config().HeartbeatTimeout / 10):
		case <-stopCh:
			return
		}

		s.peerLock.RLock()
		peer := s.peer
		s.peerLock.RUnlock()

		start := time.Now()
		if err := r.trans.AppendEntries(peer.ID, peer.Address, &req, &resp); err != nil {
			// lyf: 失败了，计算一下等待时间，限制重试频率
			// lyf: 这里和appendEntries不同的是，限定了上限，capped
			nextBackoffTime := cappedExponentialBackoff(failureWait, failures, maxFailureScale, r.config().HeartbeatTimeout/2)
			r.logger.Error("failed to heartbeat to", "peer", peer.Address, "backoff time",
				nextBackoffTime, "error", err)
			// lyf: 为啥要给所有的observer发送失败信息?这有啥优化
			r.observe(FailedHeartbeatObservation{PeerID: peer.ID, LastContact: s.LastContact()})
			failures++
			// lyf: 每个select的时候，都要考虑到兜底关闭的channel
			select {
			case <-time.After(nextBackoffTime):
			case <-stopCh:
				return
			}
		} else {
			// lyf: 恢复心跳还会给所有observer发
			if failures > 0 {
				r.observe(ResumedHeartbeatObservation{PeerID: peer.ID})
			}
			s.setLastContact()
			failures = 0
			labels := []metrics.Label{{Name: "peer_id", Value: string(peer.ID)}}
			metrics.MeasureSinceWithLabels([]string{"raft", "replication", "heartbeat"}, start, labels)
			// Duplicated information. Kept for backward compatibility.
			metrics.MeasureSince([]string{"raft", "replication", "heartbeat", string(peer.ID)}, start)
			// lyf: 还是不懂这个有啥用
			// lyf: 心跳之后，将结果告诉存储的所有verifyFuture，来验证当前还是不是leader
			// lyf: 也就是说，不传递prvLog，follower也会返回true？
			s.notifyAll(resp.Success)
		}
	}
}

// pipelineReplicate is used when we have synchronized our state with the follower,
// and want to switch to a higher performance pipeline mode of replication.
// We only pipeline AppendEntries commands, and if we ever hit an error, we fall
// back to the standard replication which can handle more complex situations.
// lyf: 高性能同步
// lyf: 好像是通过优化传输层，提高的效率?
func (r *Raft) pipelineReplicate(s *followerReplication) error {
	s.peerLock.RLock()
	peer := s.peer
	s.peerLock.RUnlock()

	// Create a new pipeline
	pipeline, err := r.trans.AppendEntriesPipeline(peer.ID, peer.Address)
	if err != nil {
		return err
	}
	defer pipeline.Close()

	// Log start and stop of pipeline
	r.logger.Info("pipelining replication", "peer", peer)
	defer r.logger.Info("aborting pipeline replication", "peer", peer)

	// Create a shutdown and finish channel
	stopCh := make(chan struct{})
	finishCh := make(chan struct{})

	// Start a dedicated decoder
	r.goFunc(func() { r.pipelineDecode(s, pipeline, stopCh, finishCh) })

	// Start pipeline sends at the last good nextIndex
	nextIndex := atomic.LoadUint64(&s.nextIndex)

	shouldStop := false
SEND:
	// lyf: 整体流程和普通的一致；只是使用了pipeline的传输层
	for !shouldStop {
		select {
		case <-finishCh:
			break SEND
		case maxIndex := <-s.stopCh:
			// Make a best effort to replicate up to this index
			if maxIndex > 0 {
				r.pipelineSend(s, pipeline, &nextIndex, maxIndex)
			}
			break SEND
		case deferErr := <-s.triggerDeferErrorCh:
			lastLogIdx, _ := r.getLastLog()
			shouldStop = r.pipelineSend(s, pipeline, &nextIndex, lastLogIdx)
			if !shouldStop {
				deferErr.respond(nil)
			} else {
				deferErr.respond(fmt.Errorf("replication failed"))
			}
		case <-s.triggerCh:
			lastLogIdx, _ := r.getLastLog()
			shouldStop = r.pipelineSend(s, pipeline, &nextIndex, lastLogIdx)
		case <-randomTimeout(r.config().CommitTimeout):
			lastLogIdx, _ := r.getLastLog()
			shouldStop = r.pipelineSend(s, pipeline, &nextIndex, lastLogIdx)
		}
	}

	// Stop our decoder, and wait for it to finish
	close(stopCh)
	select {
	case <-finishCh:
	case <-r.shutdownCh:
	}
	return nil
}

// pipelineSend is used to send data over a pipeline. It is a helper to
// pipelineReplicate.
func (r *Raft) pipelineSend(s *followerReplication, p AppendPipeline, nextIdx *uint64, lastIndex uint64) (shouldStop bool) {
	// Create a new append request
	req := new(AppendEntriesRequest)
	if err := r.setupAppendEntries(s, req, *nextIdx, lastIndex); err != nil {
		return true
	}

	// Pipeline the append entries
	if _, err := p.AppendEntries(req, new(AppendEntriesResponse)); err != nil {
		r.logger.Error("failed to pipeline appendEntries", "peer", s.peer, "error", err)
		return true
	}

	// Increase the next send log to avoid re-sending old logs
	if n := len(req.Entries); n > 0 {
		last := req.Entries[n-1]
		atomic.StoreUint64(nextIdx, last.Index+1)
	}
	return false
}

// pipelineDecode is used to decode the responses of pipelined requests.
func (r *Raft) pipelineDecode(s *followerReplication, p AppendPipeline, stopCh, finishCh chan struct{}) {
	defer close(finishCh)
	respCh := p.Consumer()
	for {
		select {
		case ready := <-respCh:
			s.peerLock.RLock()
			peer := s.peer
			s.peerLock.RUnlock()

			req, resp := ready.Request(), ready.Response()
			appendStats(string(peer.ID), ready.Start(), float32(len(req.Entries)))

			// Check for a newer term, stop running
			if resp.Term > req.Term {
				r.handleStaleTerm(s)
				return
			}

			// Update the last contact
			s.setLastContact()

			// Abort pipeline if not successful
			if !resp.Success {
				return
			}

			// Update our replication state
			updateLastAppended(s, req)
		case <-stopCh:
			return
		}
	}
}

// setupAppendEntries is used to setup an append entries request.
func (r *Raft) setupAppendEntries(s *followerReplication, req *AppendEntriesRequest, nextIndex, lastIndex uint64) error {
	req.RPCHeader = r.getRPCHeader()
	req.Term = s.currentTerm
	// this is needed for retro compatibility, before RPCHeader.Addr was added
	req.Leader = r.trans.EncodePeer(r.localID, r.localAddr)
	req.LeaderCommitIndex = r.getCommitIndex()
	// lyf: 获取之前的index和term，用于follower校验
	if err := r.setPreviousLog(req, nextIndex); err != nil {
		return err
	}
	// lyf: 获取此次传递要传送的logs
	if err := r.setNewLogs(req, nextIndex, lastIndex); err != nil {
		return err
	}
	return nil
}

// setPreviousLog is used to setup the PrevLogEntry and PrevLogTerm for an
// AppendEntriesRequest given the next index to replicate.
// lyf: 设置prvIndex和prvTerm，用于follower校验
func (r *Raft) setPreviousLog(req *AppendEntriesRequest, nextIndex uint64) error {
	// Guard for the first index, since there is no 0 log entry
	// Guard against the previous index being a snapshot as well
	// lyf: 这里保证了nextIndex不在快照中，并且保存了快照最后一个index和term
	// lyf: 所以从log里面取prvLog时，不会在快照中
	lastSnapIdx, lastSnapTerm := r.getLastSnapshot()
	if nextIndex == 1 {
		req.PrevLogEntry = 0
		req.PrevLogTerm = 0

	} else if (nextIndex - 1) == lastSnapIdx {
		// lyf: 这里记录了snapShoot最后的log的信息，这样就不用发送快照
		// lyf: 但是follower难道不需要进行快照吗？？
		req.PrevLogEntry = lastSnapIdx
		req.PrevLogTerm = lastSnapTerm

	} else {
		var l Log
		// lyf: 这是直接从持久化缓存中获取log；
		// lyf: 保证了log不会在快照中
		if err := r.logs.GetLog(nextIndex-1, &l); err != nil {
			r.logger.Error("failed to get log", "index", nextIndex-1, "error", err)
			return err
		}

		// Set the previous index and term (0 if nextIndex is 1)
		req.PrevLogEntry = l.Index
		req.PrevLogTerm = l.Term
	}
	return nil
}

// setNewLogs is used to setup the logs which should be appended for a request.
// lyf: 获取同步的log数据
func (r *Raft) setNewLogs(req *AppendEntriesRequest, nextIndex, lastIndex uint64) error {
	// Append up to MaxAppendEntries or up to the lastIndex. we need to use a
	// consistent value for maxAppendEntries in the lines below in case it ever
	// becomes reloadable.
	// lyf: 设置了一个最大传输log上限，防止新follower进cluster后，第一次传输的数据太大
	maxAppendEntries := r.config().MaxAppendEntries
	req.Entries = make([]*Log, 0, maxAppendEntries)
	maxIndex := min(nextIndex+uint64(maxAppendEntries)-1, lastIndex)
	for i := nextIndex; i <= maxIndex; i++ {
		oldLog := new(Log)
		if err := r.logs.GetLog(i, oldLog); err != nil {
			r.logger.Error("failed to get log", "index", i, "error", err)
			return err
		}
		req.Entries = append(req.Entries, oldLog)
	}
	return nil
}

// appendStats is used to emit stats about an AppendEntries invocation.
func appendStats(peer string, start time.Time, logs float32) {
	labels := []metrics.Label{{Name: "peer_id", Value: peer}}
	metrics.MeasureSinceWithLabels([]string{"raft", "replication", "appendEntries", "rpc"}, start, labels)
	metrics.IncrCounterWithLabels([]string{"raft", "replication", "appendEntries", "logs"}, logs, labels)
	// Duplicated information. Kept for backward compatibility.
	metrics.MeasureSince([]string{"raft", "replication", "appendEntries", "rpc", peer}, start)
	metrics.IncrCounter([]string{"raft", "replication", "appendEntries", "logs", peer}, logs)
}

// handleStaleTerm is used when a follower indicates that we have a stale term.
func (r *Raft) handleStaleTerm(s *followerReplication) {
	r.logger.Error("peer has newer term, stopping replication", "peer", s.peer)
	s.notifyAll(false) // No longer leader
	asyncNotifyCh(s.stepDown)
}

// updateLastAppended is used to update follower replication state after a
// successful AppendEntries RPC.
// TODO: This isn't used during InstallSnapshot, but the code there is similar.
func updateLastAppended(s *followerReplication, req *AppendEntriesRequest) {
	// Mark any inflight logs as committed
	if logs := req.Entries; len(logs) > 0 {
		last := logs[len(logs)-1]
		atomic.StoreUint64(&s.nextIndex, last.Index+1)
		// lyf: 调用commitment更新当前记录的lastIndex，并且重新计算commit
		s.commitment.match(s.peer.ID, last.Index)
	}

	// Notify still leader
	// lyf: 完成verifyFuture任务，该follower认为该node依然是leader
	s.notifyAll(true)
}
