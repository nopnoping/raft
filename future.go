// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"fmt"
	"io"
	"sync"
	"time"
)

// Future is used to represent an action that may occur in the future.
// lyf: 实现了Java里用于并发的future？
type Future interface {
	// Error blocks until the future arrives and then returns the error status
	// of the future. This may be called any number of times - all calls will
	// return the same value, however is not OK to call this method twice
	// concurrently on the same Future instance.
	// Error will only return generic errors related to raft, such
	// as ErrLeadershipLost, or ErrRaftShutdown. Some operations, such as
	// ApplyLog, may also return errors from other methods.
	// lyf: 这里只是实现了future最简单的功能
	// lyf: 提供了一个error阻塞，调用该方法后，线程将会阻塞，直到返回一个结果
	Error() error
}

// IndexFuture is used for future actions that can result in a raft log entry
// being created.
type IndexFuture interface {
	Future

	// Index holds the index of the newly applied log entry.
	// This must not be called until after the Error method has returned.
	Index() uint64
}

// ApplyFuture is used for Apply and can return the FSM response.
type ApplyFuture interface {
	IndexFuture

	// Response returns the FSM response as returned by the FSM.Apply method. This
	// must not be called until after the Error method has returned.
	// Note that if FSM.Apply returns an error, it will be returned by Response,
	// and not by the Error method, so it is always important to check Response
	// for errors from the FSM.
	Response() interface{}
}

// ConfigurationFuture is used for GetConfiguration and can return the
// latest configuration in use by Raft.
type ConfigurationFuture interface {
	IndexFuture

	// Configuration contains the latest configuration. This must
	// not be called until after the Error method has returned.
	Configuration() Configuration
}

// SnapshotFuture is used for waiting on a user-triggered snapshot to complete.
type SnapshotFuture interface {
	Future

	// Open is a function you can call to access the underlying snapshot and
	// its metadata. This must not be called until after the Error method
	// has returned.
	Open() (*SnapshotMeta, io.ReadCloser, error)
}

// LeadershipTransferFuture is used for waiting on a user-triggered leadership
// transfer to complete.
type LeadershipTransferFuture interface {
	Future
}

// errorFuture is used to return a static error.
type errorFuture struct {
	err error
}

func (e errorFuture) Error() error {
	return e.err
}

func (e errorFuture) Response() interface{} {
	return nil
}

func (e errorFuture) Index() uint64 {
	return 0
}

// deferError can be embedded to allow a future
// to provide an error in the future.
// lyf: 给那些等待提交成功的类，提供错误服务？？
type deferError struct {
	// lyf: 记录错误
	err error
	// lyf: 错误channel，用它就可以实现调用Error进入阻塞
	errCh chan error
	// lyf: 记录该future是否已经返回了
	responded bool
	// lyf: shutdown？用来兜底吗？
	ShutdownCh chan struct{}
}

func (d *deferError) init() {
	d.errCh = make(chan error, 1)
}

func (d *deferError) Error() error {
	// lyf: patch第二次调用
	if d.err != nil {
		// Note that when we've received a nil error, this
		// won't trigger, but the channel is closed after
		// send so we'll still return nil below.
		return d.err
	}
	// lyf: 这个为nil，表示没有初始化，使用panic跑出异常
	// lyf: go中使用panic抛出异常，然后在defer中使用recover来捕获
	if d.errCh == nil {
		panic("waiting for response on nil channel")
	}
	// lyf: 阻塞，直到channel中有数据
	select {
	case d.err = <-d.errCh:
	case <-d.ShutdownCh:
		d.err = ErrRaftShutdown
	}
	return d.err
}

func (d *deferError) respond(err error) {
	if d.errCh == nil {
		return
	}
	if d.responded {
		return
	}
	// lyf: 发送error，并关闭
	d.errCh <- err
	close(d.errCh)
	d.responded = true
}

// There are several types of requests that cause a configuration entry to
// be appended to the log. These are encoded here for leaderLoop() to process.
// This is internal to a single server.
// lyf: 配置变更future
type configurationChangeFuture struct {
	logFuture
	req configurationChangeRequest
}

// bootstrapFuture is used to attempt a live bootstrap of the cluster. See the
// Raft object's BootstrapCluster member function for more details.
type bootstrapFuture struct {
	deferError

	// configuration is the proposed bootstrap configuration to apply.
	configuration Configuration
}

// logFuture is used to apply a log entry and waits until
// the log is considered committed.
// lyf: 等待一个log提交成功
type logFuture struct {
	// lyf: 结构体的内嵌
	deferError
	log Log
	// lyf: 该future的结果
	response interface{}
	// lyf: 记录和follower同步的时间
	dispatch time.Time
}

func (l *logFuture) Response() interface{} {
	return l.response
}

func (l *logFuture) Index() uint64 {
	return l.log.Index
}

type shutdownFuture struct {
	raft *Raft
}

func (s *shutdownFuture) Error() error {
	if s.raft == nil {
		return nil
	}
	s.raft.waitShutdown()
	if closeable, ok := s.raft.trans.(WithClose); ok {
		closeable.Close()
	}
	return nil
}

// userSnapshotFuture is used for waiting on a user-triggered snapshot to
// complete.
type userSnapshotFuture struct {
	deferError

	// opener is a function used to open the snapshot. This is filled in
	// once the future returns with no error.
	opener func() (*SnapshotMeta, io.ReadCloser, error)
}

// Open is a function you can call to access the underlying snapshot and its
// metadata.
func (u *userSnapshotFuture) Open() (*SnapshotMeta, io.ReadCloser, error) {
	if u.opener == nil {
		return nil, nil, fmt.Errorf("no snapshot available")
	}
	// Invalidate the opener so it can't get called multiple times,
	// which isn't generally safe.
	defer func() {
		u.opener = nil
	}()
	return u.opener()
}

// userRestoreFuture is used for waiting on a user-triggered restore of an
// external snapshot to complete.
type userRestoreFuture struct {
	deferError

	// meta is the metadata that belongs with the snapshot.
	meta *SnapshotMeta

	// reader is the interface to read the snapshot contents from.
	reader io.Reader
}

// reqSnapshotFuture is used for requesting a snapshot start.
// It is only used internally.
type reqSnapshotFuture struct {
	deferError

	// snapshot details provided by the FSM runner before responding
	index    uint64
	term     uint64
	snapshot FSMSnapshot
}

// restoreFuture is used for requesting an FSM to perform a
// snapshot restore. Used internally only.
type restoreFuture struct {
	deferError
	ID string
}

// verifyFuture is used to verify the current node is still
// the leader. This is to prevent a stale read.
type verifyFuture struct {
	deferError
	notifyCh   chan *verifyFuture
	quorumSize int
	votes      int
	voteLock   sync.Mutex
}

// leadershipTransferFuture is used to track the progress of a leadership
// transfer internally.
type leadershipTransferFuture struct {
	deferError

	ID      *ServerID
	Address *ServerAddress
}

// configurationsFuture is used to retrieve the current configurations. This is
// used to allow safe access to this information outside of the main thread.
type configurationsFuture struct {
	deferError
	configurations configurations
}

// Configuration returns the latest configuration in use by Raft.
func (c *configurationsFuture) Configuration() Configuration {
	return c.configurations.latest
}

// Index returns the index of the latest configuration in use by Raft.
func (c *configurationsFuture) Index() uint64 {
	return c.configurations.latestIndex
}

// vote is used to respond to a verifyFuture.
// This may block when responding on the notifyCh.
// lyf: 该follower是否认可该node为leader
func (v *verifyFuture) vote(leader bool) {
	v.voteLock.Lock()
	defer v.voteLock.Unlock()

	// Guard against having notified already
	if v.notifyCh == nil {
		return
	}

	if leader {
		v.votes++
		if v.votes >= v.quorumSize {
			v.notifyCh <- v
			v.notifyCh = nil
		}
	} else {
		// lyf: 当follower不认为它是leader时，将会提前发送信息给verifyCh
		v.notifyCh <- v
		v.notifyCh = nil
	}
}

// appendFuture is used for waiting on a pipelined append
// entries RPC.
type appendFuture struct {
	deferError
	start time.Time
	args  *AppendEntriesRequest
	resp  *AppendEntriesResponse
}

func (a *appendFuture) Start() time.Time {
	return a.start
}

func (a *appendFuture) Request() *AppendEntriesRequest {
	return a.args
}

func (a *appendFuture) Response() *AppendEntriesResponse {
	return a.resp
}
