package shardmaster

import (
	"bytes"
	"encoding/gob"
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/assert"
)

func hashInt(key int) int {
	buff := new(bytes.Buffer)
	encoder := gob.NewEncoder(buff)
	if err := encoder.Encode(key); err != nil {
		panic(err)
	}
	return int(crc32.ChecksumIEEE(buff.Bytes()))
}

func TestHash(t *testing.T) {
	hash := NewHash(3, hashInt)
	hash.Add([]int{1})

	assert.Equal(t, 1, hash.Hash(1))
	assert.Equal(t, 1, hash.Hash(2))
	assert.Equal(t, 1, hash.Hash(3))
	assert.Equal(t, 1, hash.Hash(4))

	hash.Add([]int{2})
	t.Logf("%d", hash.Hash(1))
	t.Logf("%d", hash.Hash(2))
	t.Logf("%d", hash.Hash(3))
	t.Logf("%d", hash.Hash(4))
	t.Logf("%d", hash.Hash(5))
	t.Logf("%d", hash.Hash(6))
	t.Logf("%d", hash.Hash(7))
	t.Logf("%d", hash.Hash(8))
}
