// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"sort"
	"sync"
)

// Commitment is used to advance the leader's commit index. The leader and
// replication goroutines report in newly written entries with match(), and
// this notifies on commitCh when the commit index has advanced.
// lyf: 用来计算commitIndex
type commitment struct {
	// protects matchIndexes and commitIndex
	sync.Mutex
	// notified when commitIndex increases
	// lyf: 这个channel和leaderState中的是一个吗？
	commitCh chan struct{}
	// voter ID to log index: the server stores up through this log entry
	// lyf: 每个follower的匹配状态
	matchIndexes map[ServerID]uint64
	// a quorum stores up through this log entry. monotonically increases.
	// lyf: commit的值
	commitIndex uint64
	// the first index of this leader's term: this needs to be replicated to a
	// majority of the cluster before this leader may mark anything committed
	// (per Raft's commitment rule)
	// lyf: 这个leader开始时，提交的nop的index
	startIndex uint64
}

// newCommitment returns a commitment struct that notifies the provided
// channel when log entries have been committed. A new commitment struct is
// created each time this server becomes leader for a particular term.
// 'configuration' is the servers in the cluster.
// 'startIndex' is the first index created in this term (see
// its description above).
func newCommitment(commitCh chan struct{}, configuration Configuration, startIndex uint64) *commitment {
	matchIndexes := make(map[ServerID]uint64)
	for _, server := range configuration.Servers {
		if server.Suffrage == Voter {
			matchIndexes[server.ID] = 0
		}
	}
	return &commitment{
		commitCh:     commitCh,
		matchIndexes: matchIndexes,
		commitIndex:  0,
		startIndex:   startIndex,
	}
}

// Called when a new cluster membership configuration is created: it will be
// used to determine commitment from now on. 'configuration' is the servers in
// the cluster.
// lyf: 根据配置信息，重新构建commitment的match信息
func (c *commitment) setConfiguration(configuration Configuration) {
	c.Lock()
	defer c.Unlock()
	oldMatchIndexes := c.matchIndexes
	c.matchIndexes = make(map[ServerID]uint64)
	for _, server := range configuration.Servers {
		if server.Suffrage == Voter {
			c.matchIndexes[server.ID] = oldMatchIndexes[server.ID] // defaults to 0
		}
	}
	c.recalculate()
}

// Called by leader after commitCh is notified
func (c *commitment) getCommitIndex() uint64 {
	c.Lock()
	defer c.Unlock()
	return c.commitIndex
}

// Match is called once a server completes writing entries to disk: either the
// leader has written the new entry or a follower has replied to an
// AppendEntries RPC. The given server's disk agrees with this server's log up
// through the given index.
// lyf: 用来更新同步的lastIndex
func (c *commitment) match(server ServerID, matchIndex uint64) {
	c.Lock()
	defer c.Unlock()
	if prev, hasVote := c.matchIndexes[server]; hasVote && matchIndex > prev {
		c.matchIndexes[server] = matchIndex
		c.recalculate()
	}
}

// Internal helper to calculate new commitIndex from matchIndexes.
// Must be called with lock held.
// lyf: 计算当前的commit；方法很暴力，排序后取中间
func (c *commitment) recalculate() {
	if len(c.matchIndexes) == 0 {
		return
	}

	matched := make([]uint64, 0, len(c.matchIndexes))
	for _, idx := range c.matchIndexes {
		matched = append(matched, idx)
	}
	sort.Sort(uint64Slice(matched))
	quorumMatchIndex := matched[(len(matched)-1)/2]

	if quorumMatchIndex > c.commitIndex && quorumMatchIndex >= c.startIndex {
		c.commitIndex = quorumMatchIndex
		// lyf: 非阻塞的通知；为什么用非阻塞的？
		// lyf: 可能会有多个在通知；只要有一个通知消息没有被消费，就是一样的，所以就用非阻塞的
		asyncNotifyCh(c.commitCh)
	}
}
