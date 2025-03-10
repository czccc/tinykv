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
	"sort"

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
	hard, conf, err := c.Storage.InitialState()
	if err != nil {
		panic(err.Error())
	}
	r := Raft{
		id:               c.ID,
		Term:             hard.Term,
		Vote:             hard.Vote,
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
	r.RaftLog.committed = hard.Commit
	r.votes = make(map[uint64]bool)
	r.randomizedTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	if c.peers != nil && conf.Nodes != nil {
		panic("cannot specify both newRaft(peers) and ConfState.Nodes")
	}
	if c.peers != nil {
		for _, i := range c.peers {
			r.Prs[uint64(i)] = &Progress{
				Match: c.Applied,
				Next:  c.Applied + 1,
			}
		}
	}
	if conf.Nodes != nil {
		for _, i := range conf.Nodes {
			r.Prs[uint64(i)] = &Progress{
				Match: c.Applied,
				Next:  c.Applied + 1,
			}
		}
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
	r.State = StateFollower
	r.Term = term
	r.Vote = lead
	r.Lead = lead
	r.votes = make(map[uint64]bool)
	r.electionElapsed = 0
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term++
	r.Vote = r.id
	r.Lead = None
	r.votes = make(map[uint64]bool)
	r.electionElapsed = 0
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
	r.Step(pb.Message{
		From:    r.id,
		To:      r.id,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{{}},
	})
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
		r.handleRequestVote(m)
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
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			Reject:  true,
		}, m.From)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
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
		r.bcastHeartbeat()
	case pb.MessageType_MsgPropose:
		r.handlePropose(m)
	case pb.MessageType_MsgAppend:
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntriesResponse(m)
	case pb.MessageType_MsgRequestVote:
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			Reject:  true,
		}, m.From)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgHeartbeat:
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:
	}
	return nil
}

func (r *Raft) campaign() {
	r.becomeCandidate()
	for i := range r.Prs {
		if i != r.id {
			li := r.RaftLog.LastIndex()
			lt, err := r.RaftLog.Term(li)
			if err != nil {
				panic(err)
			}
			msg := pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				Index:   li,
				LogTerm: lt,
			}
			r.send(msg, i)
		}
	}
}

func (r *Raft) checkMatch() {
	match := make([]uint64, 0, len(r.Prs))
	for i := range r.Prs {
		match = append(match, r.Prs[i].Match)
	}
	sort.Slice(match, func(i, j int) bool {
		return match[i] < match[j]
	})
	matched := match[(len(match)+1)/2-1]
	matchedTerm, err := r.RaftLog.Term(matched)
	if err != nil {
		panic(err)
	}
	if matchedTerm == r.Term && matched > r.RaftLog.committed {
		r.RaftLog.committed = matched
		r.bcastAppend()
	}
}

func (r *Raft) handlePropose(m pb.Message) {
	for _, ent := range m.Entries {
		r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
			EntryType: pb.EntryType_EntryNormal,
			Index:     r.RaftLog.LastIndex() + 1,
			Term:      r.Term,
			Data:      ent.Data,
		})
	}
	r.Step(pb.Message{
		From:    r.id,
		To:      r.id,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgAppendResponse,
		Reject:  false,
		Index:   r.RaftLog.LastIndex(),
	})
	r.bcastAppend()
}

func (r *Raft) handleRequestVote(m pb.Message) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		Reject:  false,
	}
	if r.Vote != None && r.Vote != m.From {
		msg.Reject = true
	}
	li := r.RaftLog.LastIndex()
	lt, err := r.RaftLog.Term(li)
	if err != nil {
		panic(err)
	}
	if m.Term == r.Term && (m.LogTerm < lt || (m.LogTerm == lt && m.Index < li)) {
		msg.Reject = true
	}
	if !msg.Reject {
		r.Vote = m.From
	}
	r.send(msg, m.From)
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	count := 0
	reject := 0
	r.votes[m.From] = !m.Reject
	for _, v := range r.votes {
		if v {
			count++
		} else {
			reject++
		}
	}
	if count > len(r.Prs)/2 {
		r.becomeLeader()
	}
	if reject > len(r.Prs)/2 {
		r.becomeFollower(m.Term, None)
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
	} else {
		ents := []*pb.Entry{}
		for i := pr.Next - r.RaftLog.snapshotIndex - 1; i < uint64(len(r.RaftLog.entries)); i++ {
			ents = append(ents, &r.RaftLog.entries[i])
		}
		lt, err := r.RaftLog.Term(pr.Next - 1)
		if err != nil {
			panic(err)
			// log.Fatal(err)
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

func (r *Raft) bcastAppend() {
	for i := range r.Prs {
		if i != r.id {
			r.sendAppend(i)
		}
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		Reject:  false,
	}
	if m.Term < r.Term {
		msg.Reject = true
	} else {
		r.Lead = m.From
		match, err := r.RaftLog.Term(m.Index)
		if err != nil || m.LogTerm != match {
			msg.Reject = true
		} else {
			if r.RaftLog.LastIndex() == m.Index {
				for _, ent := range m.Entries {
					r.RaftLog.entries = append(r.RaftLog.entries, *ent)
				}
			} else {
				for i, ent := range m.Entries {
					term, err := r.RaftLog.Term(ent.Index)
					if err != nil || term != ent.Term {
						r.RaftLog.entries = r.RaftLog.entries[:ent.Index-r.RaftLog.snapshotIndex-1]
						r.RaftLog.stabled = ent.Index - 1
						for _, ent := range m.Entries[i:] {
							r.RaftLog.entries = append(r.RaftLog.entries, *ent)
						}
						break
					}
				}
			}
			msg.Index = r.RaftLog.LastIndex()
			r.RaftLog.committed = min(min(r.RaftLog.LastIndex(), m.Commit), m.Index+uint64(len(m.Entries)))
		}
	}
	r.send(msg, m.From)
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if m.Reject {
		r.Prs[m.From].Next -= 1
		r.sendAppend(m.From)
	} else {
		r.Prs[m.From].Match = max(r.Prs[m.From].Match, m.Index)
		r.Prs[m.From].Next = m.Index + 1
		r.checkMatch()
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

func (r *Raft) bcastHeartbeat() {
	for i := range r.Prs {
		if i != r.id {
			r.sendHeartbeat(i)
		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		Reject:  false,
		Index:   r.RaftLog.LastIndex(),
	}
	if m.Term < r.Term {
		msg.Reject = true
	}
	if m.Commit > r.RaftLog.committed {
		msg.Reject = true
	}
	r.electionElapsed = 0
	r.send(msg, m.From)
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if !m.Reject {
		r.Prs[m.From].Match = max(r.Prs[m.From].Match, m.Index)
		r.Prs[m.From].Next = m.Index + 1
		r.checkMatch()
	} else {
		r.sendAppend(m.From)
	}
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
