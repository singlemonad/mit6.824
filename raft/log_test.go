package raft

import (
	"bytes"
	"encoding/gob"
	"testing"

	"github.com/stretchr/testify/assert"
)

func materInt(encoder *gob.Encoder, command interface{}) error {
	val := command.(int)
	return encoder.Encode(val)
}

func deMaterInt(decoder *gob.Decoder) (interface{}, error) {
	var command int
	err := decoder.Decode(&command)
	return command, err
}

func TestPersist(t *testing.T) {
	l := NewMemoryLog(nil, materInt, deMaterInt)
	l.Append([]*Entry{
		&Entry{0, 1, 100},
	})
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	l.Persist(e)
	data := w.Bytes()

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	l2 := NewMemoryLog(d, materInt, deMaterInt)
	assert.Equal(t, l.GetLogs(0)[0], l2.GetLogs(0)[0])
}
