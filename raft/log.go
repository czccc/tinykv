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
	"fmt"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	snapshotIndex uint64
	snapshotTerm  uint64
}

func (l *RaftLog) String() string {
	return fmt.Sprintf("[RaftLog Term %d Index %d][Snap %d %d][Ents %d][A %d C %d S %d]",
		l.LastTerm(), l.LastIndex(),
		l.snapshotIndex, l.snapshotTerm, len(l.entries), l.applied, l.committed, l.stabled)
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	entries := make([]pb.Entry, 0)
	if lastIndex >= firstIndex {
		entries, err = storage.Entries(firstIndex, lastIndex+1)
		if err != nil {
			panic(err)
		}
	}
	snapshotTerm, err := storage.Term(firstIndex - 1)
	if err != nil {
		panic(err)
	}

	l := RaftLog{
		storage:       storage,
		committed:     firstIndex - 1,
		applied:       firstIndex - 1,
		stabled:       lastIndex,
		entries:       entries,
		snapshotIndex: firstIndex - 1,
		snapshotTerm:  snapshotTerm,
	}
	return &l
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.entries[:]
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.entries[l.stabled-l.snapshotIndex:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	for i := l.applied + 1; i <= l.committed; i++ {
		ents = append(ents, *l.getEntry(i))
	}
	return ents
}

// Return entries with index in [from, to).
func (l *RaftLog) getEntries(from, to uint64) (ents []*pb.Entry) {
	if from <= l.snapshotIndex {
		log.Fatalf("%s `from` less than min log index! from=%d, min_index=%d", l, from, l.snapshotIndex)
	}
	if to > l.LastIndex()+1 {
		log.Fatalf("%s `to` exceed max log index! to=%d, max_index=%d", l, to, l.snapshotIndex+uint64(len(l.entries)))
	}
	if from > to {
		log.Debugf("%s rangeEntries from > to! return empty entries! from=%d, to=%d", l, from, to)
		return ents
	}
	for i := from; i < to; i++ {
		ents = append(ents, l.getEntry(i))
	}
	return ents
}

// Return the log entry in given index
func (l *RaftLog) getEntry(index uint64) (ent *pb.Entry) {
	if index <= l.snapshotIndex {
		log.Fatalf("%s Found [index %d] <= [l.snapshotIndex %d]", l, index, l.snapshotIndex)
	}
	if index > l.LastIndex() {
		log.Fatalf("%s Found [index %d] > [l.LastIndex() %d]", l, index, l.LastIndex())
	}
	return &l.entries[index-l.snapshotIndex-1]
}

func (l *RaftLog) appendEntries(prevIndex uint64, ents []*pb.Entry) {
	if prevIndex < l.snapshotIndex {
		log.Fatalf("%s invalid prevIndex=%d", l, prevIndex)
	} else if prevIndex < l.LastIndex() {
		for i, ent := range ents {
			term, err := l.Term(ent.Index)
			if err != nil || term != ent.Term {
				l.entries = l.entries[:ent.Index-l.snapshotIndex-1]
				l.stableTo(ent.Index - 1)
				for _, ent := range ents[i:] {
					l.entries = append(l.entries, *ent)
				}
				break
			}
		}
	} else if prevIndex == l.LastIndex() {
		for _, ent := range ents {
			l.entries = append(l.entries, *ent)
		}
	} else {
		log.Fatalf("%s invalid prevIndex=%d", l, prevIndex)
	}
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	return uint64(len(l.entries)) + l.snapshotIndex
}

// LastTerm return the last term of the log entries
func (l *RaftLog) LastTerm() uint64 {
	if len(l.entries) > 0 {
		return l.entries[len(l.entries)-1].Term
	}
	return l.snapshotTerm
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i < l.snapshotIndex {
		return 0, ErrCompacted
	} else if i > l.LastIndex() {
		return 0, ErrUnavailable
	} else if i == l.snapshotIndex {
		return l.snapshotTerm, nil
	} else {
		return l.getEntry(i).Term, nil
	}
}

func (l *RaftLog) applyTo(index uint64) {
	if index > l.LastIndex() || index < l.applied || index > l.committed {
		log.Fatalf("%s invalid applied index, index=%d", l, index)
	}
	l.applied = index
}

func (l *RaftLog) commitTo(index uint64) {
	if index > l.LastIndex() || index < l.committed {
		log.Fatalf("%s invalid committed index, index=%d", l, index)
	}
	l.committed = index
}

func (l *RaftLog) stableTo(index uint64) {
	if index > l.LastIndex() || index < l.committed {
		log.Fatalf("%s invalid applied index, index=%d", l, index)
	}
	l.stabled = index
}
