package raft

import (
	"encoding/gob"
)

type Entry struct {
	Index int
	Term  int
	Data  interface{}
}

type RaftLog interface {
	FirstIndex() int
	LastTerm() int
	LastIndex() int
	Match(logIndex, logTerm int) bool
	IsUptoDate(lastIndex, lastTerm int) bool
	GetLogTerm(index int) int
	GetLog(index int) *Entry
	GetLogs(startIndex int) []*Entry
	Append(entries []*Entry)
	GetMaxMatchIndex(term int) int
	Catoff(index int)
	Persist(e *gob.Encoder)
	RemoveExpiredLogs(endIndex int)
}

type MaterializeFunc = func(encoder *gob.Encoder, command interface{}) error
type DeMaterializeFunc = func(decoder *gob.Decoder) (interface{}, error)

// MemoryLog is raft log in memory, not persistent to disk
// entries[0] not use, log start at entries[1]
type MemoryLog struct {
	len               int
	entries           []*Entry
	materializeFunc   MaterializeFunc
	deMaterializeFunc DeMaterializeFunc
}

func NewMemoryLog(d *gob.Decoder, materFunc MaterializeFunc, deMaterFunc DeMaterializeFunc) RaftLog {
	l := &MemoryLog{
		entries:           make([]*Entry, 0),
		materializeFunc:   materFunc,
		deMaterializeFunc: deMaterFunc,
	}
	if d != nil {
		// decode log length
		if err := d.Decode(&l.len); err != nil {
			panic(err.Error())
		}

		// decode log one by one
		for i := 0; i < l.len; i++ {
			var index, term int
			if err := d.Decode(&index); err != nil {
				panic(err)
			}
			if err := d.Decode(&term); err != nil {
				panic(err)
			}
			command, err := deMaterFunc(d)
			if err != nil {
				panic(err)
			}
			l.entries = append(l.entries, &Entry{index, term, command})
		}
	}
	return l
}

func (l *MemoryLog) FirstIndex() int {
	if l.len == 0 {
		return -1
	}
	return l.entries[0].Index
}

func (l *MemoryLog) LastTerm() int {
	if l.len == 0 {
		return -1
	}
	return l.entries[l.len-1].Term
}

func (l *MemoryLog) LastIndex() int {
	return int(l.len - 1)
}

func (l *MemoryLog) Match(logIndex, logTerm int) bool {
	if logIndex >= l.len {
		return false
	}

	if logIndex == -1 {
		if logTerm == -1 {
			return true
		}
		return false
	}
	return l.entries[logIndex].Term == logTerm
}

func (l *MemoryLog) IsUptoDate(lastIndex, lastTerm int) bool {
	//if lastIndex >= l.len {
	//	return false
	//}

	if lastTerm > l.LastTerm() {
		return true
	} else if lastTerm < l.LastTerm() {
		return false
	} else {
		if lastIndex >= l.LastIndex() {
			return true
		}
		return false
	}
}

func (l *MemoryLog) GetLogTerm(index int) int {
	if index >= l.len {
		return -1
	}
	if index == -1 {
		return -1
	}
	return l.entries[index].Term
}

func (l *MemoryLog) GetLog(index int) *Entry {
	if index >= l.len {
		return nil
	}
	return l.entries[index]
}

func (l *MemoryLog) GetLogs(startIndex int) []*Entry {
	if startIndex >= l.len {
		return nil
	}
	return l.entries[startIndex:l.len]
}

func (l *MemoryLog) Append(entries []*Entry) {
	l.entries = append(l.entries, entries...)
	l.len += len(entries)
}

func (l *MemoryLog) GetMaxMatchIndex(term int) int {
	for index := l.len - 1; index >= 0; index-- {
		if l.entries[index].Term == term {
			return index
		}

		if l.entries[index].Term < term {
			return -1
		}
	}
	return -1
}

func (l *MemoryLog) Catoff(index int) {
	l.entries = l.entries[0:index]
	l.len = len(l.entries)
}

func (l *MemoryLog) Persist(e *gob.Encoder) {
	// write log len
	if err := e.Encode(l.len); err != nil {
		panic(err.Error())
	}
	// write log one by one
	for i := 0; i < l.len; i++ {
		if err := e.Encode(l.entries[i].Index); err != nil {
			panic(err.Error())
		}
		if err := e.Encode(l.entries[i].Term); err != nil {
			panic(err.Error())
		}
		if err := l.materializeFunc(e, l.entries[i].Data); err != nil {
			panic(err)
		}
	}
}

func (l *MemoryLog) RemoveExpiredLogs(endIndex int) {
	l.entries = l.entries[endIndex+1 : len(l.entries)]
	l.len = len(l.entries)
}
