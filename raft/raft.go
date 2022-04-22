// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"fmt"
	"math/rand"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout   int
	randomizedTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	r := Raft{
		id:               c.ID,
		Term:             0,
		Vote:             None,
		RaftLog:          newLog(c.Storage),
		Prs:              make(map[uint64]*Progress),
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		msgs:             make([]pb.Message, 0),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,
		leadTransferee:   None,
		PendingConfIndex: None,
	}
	r.randomizedTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	for i := range c.peers {
		r.Prs[uint64(i+1)] = &Progress{
			Match: c.Applied,
			Next:  c.Applied + 1,
		}
		r.votes[uint64(i+1)] = false
	}
	log.Debugf("Raft %s created, peers: %d", r.String(), len(r.Prs))
	return &r
}

func (r *Raft) String() string {
	return fmt.Sprintf("[%c %3d %3d]", r.State.String()[5], r.id, r.Term)
}

func (r *Raft) send(m pb.Message, to uint64) {
	// log.Debugf("%s send %s to %d", r.String(), m.String(), to)
	m.From = r.id
	m.To = to
	m.Term = r.Term
	r.msgs = append(r.msgs, m)
}

func (r *Raft) appendEntries(ents []*pb.Entry) {
	if len(ents) == 0 {
		return
	}
	for _, e := range ents {
		r.RaftLog.entries = append(r.RaftLog.entries, *e)
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	pr := r.Prs[to]
	if pr.Next <= r.RaftLog.snapshotIndex {
		log.Fatal("Unimplemented")
		return false
	} else if pr.Next > r.RaftLog.LastIndex() {
		return false
	} else {
		ents := []*pb.Entry{}
		for i := pr.Next - r.RaftLog.snapshotIndex; i <= uint64(len(r.RaftLog.entries)); i++ {
			ents = append(ents, &r.RaftLog.entries[i])
		}
		lt, err := r.RaftLog.Term(pr.Next - 1)
		if err != nil {
			log.Fatal(err)
		}
		m := pb.Message{
			MsgType: pb.MessageType_MsgAppend,
			Entries: ents,
			Index:   pr.Next - 1,
			LogTerm: lt,
			Commit:  r.RaftLog.committed,
		}
		r.send(m, to)
		return true
	}
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	m := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		Commit:  r.RaftLog.committed,
	}
	r.send(m, to)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.tickElection()
	case StateCandidate:
		r.tickElection()
	case StateLeader:
		r.tickHeartbeat()
	}
}

func (r *Raft) tickElection() {
	r.electionElapsed++
	if r.electionElapsed >= r.randomizedTimeout {
		r.electionElapsed = 0
		r.randomizedTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
		r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgHup})
	}
}

func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgBeat})
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.Term = term
	r.Vote = lead
	for i := range r.Prs {
		r.votes[i] = false
	}
	r.State = StateFollower
	r.Lead = lead
	r.electionElapsed = 0
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.Term++
	r.Vote = r.id
	r.State = StateCandidate
	r.Lead = None
	r.electionElapsed = 0
	for i := range r.Prs {
		r.votes[i] = false
	}
	r.Step(pb.Message{From: r.id, To: r.id, Term: r.Term, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: false})
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term

	r.State = StateLeader
	r.Lead = r.id
	for i := range r.Prs {
		r.Prs[i].Next = r.RaftLog.LastIndex() + 1
		r.Prs[i].Match = r.RaftLog.committed
	}

}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	// log.Debugf("Raft %s received %v", r.String(), m)

	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}
	switch r.State {
	case StateFollower:
		return r.stepFollower(m)
	case StateCandidate:
		return r.stepCandidate(m)
	case StateLeader:
		return r.stepLeader(m)
	}
	return nil
}

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.campaign()
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
		r.send(m, r.Lead)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		reject := false
		if r.Vote != None && r.Vote != m.From {
			reject = true
		}
		if !reject {
			r.Vote = m.From
		}
		msg := pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			Reject:  reject,
		}
		r.send(msg, m.From)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.campaign()
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
		r.stepFollower(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
	case pb.MessageType_MsgRequestVoteResponse:
		if !m.Reject {
			r.votes[m.From] = true
			count := 0
			for _, v := range r.votes {
				if v {
					count++
				}
			}
			if count > len(r.Prs)/2 {
				r.becomeLeader()
				r.Step(pb.Message{MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}})
			}
		}
	case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.stepFollower(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:
	}
	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
	case pb.MessageType_MsgBeat:
		for i := range r.Prs {
			if i != r.id {
				msg := pb.Message{
					MsgType: pb.MessageType_MsgHeartbeat,
				}
				r.send(msg, i)
			}
		}
	case pb.MessageType_MsgPropose:
		for _, ent := range m.Entries {
			r.RaftLog.entries = append(r.RaftLog.entries, *ent)
		}
	case pb.MessageType_MsgAppend:
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgHeartbeat:
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:
	}
	return nil
}

func (r *Raft) campaign() {
	r.becomeCandidate()
	for i := range r.Prs {
		if i != r.id {
			msg := pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
			}
			r.send(msg, i)
		}
	}
}

func (r *Raft) handleRequestVote(m pb.Message) {
	var reject = false
	if m.Term < r.Term || r.Vote != None && r.Vote != m.From {
		reject = true
	}
	li := r.RaftLog.LastIndex()
	lt, err := r.RaftLog.Term(li)
	if err != nil {
		log.Errorf("Raft %s handleRequestVote: %v", r.String(), err)
	}
	if m.LogTerm < lt || (m.LogTerm == lt && m.Index < li) {
		reject = true
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		Reject:  reject,
	}
	r.send(msg, m.From)
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	var reject = false
	if m.Term < r.Term {
		reject = true
	} else {
		match, err := r.RaftLog.Term(m.Index)
		if err != nil || m.LogTerm != match {
			reject = true
		} else {
			r.RaftLog.entries = r.RaftLog.entries[:m.Index-r.RaftLog.snapshotIndex]
		}
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		Reject:  reject,
	}
	r.send(msg, m.From)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
