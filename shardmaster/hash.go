package shardmaster

import "sort"

type hashFunc = func(int) int

type Hash struct {
	replicas int
	hashF    hashFunc
	hashMap  map[int]int
	keys     []int
}

func NewHash(replicas int, fn hashFunc) *Hash {
	return &Hash{
		replicas: replicas,
		hashF:    fn,
		hashMap:  make(map[int]int),
		keys:     make([]int, 0),
	}
}

func (h *Hash) Add(servers []int) {
	for _, server := range servers {
		for replica := 0; replica < h.replicas; replica++ {
			key := h.hashF(replica + server)
			if _, ok := h.hashMap[key]; !ok {
				h.hashMap[key] = server
				h.keys = append(h.keys, key)
			}
		}
	}
	sort.Slice(h.keys, func(i, j int) bool {
		return h.keys[i] < h.keys[j]
	})
}

func (h *Hash) Delete(servers []int) {
	for _, server := range servers {
		for replica := 0; replica < h.replicas; replica++ {
			key := h.hashF(replica + server)
			if val, ok := h.hashMap[key]; ok && val == server {
				delete(h.hashMap, key)
			}
		}
	}
	h.keys = make([]int, 0)
	for key, _ := range h.hashMap {
		h.keys = append(h.keys, key)
	}
	sort.Slice(h.keys, func(i, j int) bool {
		return h.keys[i] < h.keys[j]
	})
}

func (h *Hash) Hash(key int) (server int) {
	if len(h.keys) == 0 {
		return 0
	}
	hash := h.hashF(key)
	idx := sort.Search(len(h.keys), func(i int) bool {
		return h.keys[i] >= hash
	})
	if idx == len(h.keys) {
		idx = 0
	}
	return h.hashMap[h.keys[idx]]
}
