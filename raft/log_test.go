package raft

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPersist(t *testing.T) {
	l := NewMemoryLog(nil)
	l.Append([]*Entry{
		&Entry{1, 100},
	})
	bytes := l.Persist()
	l2 := NewMemoryLog(bytes)
	assert.Equal(t, l.GetLogs(0)[0], l2.GetLogs(0)[0])
}
