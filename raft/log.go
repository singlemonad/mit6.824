package raft

import (
	"bytes"
	"encoding/gob"
)

type Entry struct {
	Term int
	Data interface{}
}

type RaftLog interface {
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
	Persist() []byte
}

// MemoryLog is raft log in memory, not persistent to disk
// entries[0] not use, log start at entries[1]
type MemoryLog struct {
	len     int // len is log length, equal len(entries) - 1
	entries []*Entry
}

func NewMemoryLog(content []byte) RaftLog {
	l := &MemoryLog{
		entries: make([]*Entry, 0),
	}
	if content != nil {
		r := bytes.NewBuffer(content)
		d := gob.NewDecoder(r)

		// decode log length
		if err := d.Decode(&l.len); err != nil {
			panic(err.Error())
		}

		// decode log one by one
		for i := 0; i < l.len; i++ {
			var term int
			var data int

			if err := d.Decode(&term); err != nil {
				panic(err.Error())
			}
			if err := d.Decode(&data); err != nil {
				panic(err.Error())
			}
			l.entries = append(l.entries, &Entry{term, data})
		}
	}
	return l
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

func (l *MemoryLog) Persist() []byte {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	// write log len
	if err := e.Encode(l.len); err != nil {
		panic(err.Error())
	}
	// write log one by one
	for i := 0; i < l.len; i++ {
		if err := e.Encode(l.entries[i].Term); err != nil {
			panic(err.Error())
		}
		if err := e.Encode(l.entries[i].Data); err != nil {
			panic(err.Error())
		}
	}

	return w.Bytes()
}
