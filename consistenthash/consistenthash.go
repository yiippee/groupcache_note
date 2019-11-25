/*
Copyright 2013 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package consistenthash provides an implementation of a ring hash.
package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

type Hash func(data []byte) uint32

type Map struct {
	hash     Hash           //一致性hash函数
	replicas int            //单一节点在一致性hash map中的虚拟节点数
	keys     []int          // Sorted //所有节点生成的虚拟节点hash值slice,排序了的
	hashMap  map[int]string // hash值和节点对应map
}

func New(replicas int, fn Hash) *Map {
	m := &Map{
		replicas: replicas,
		hash:     fn,
		hashMap:  make(map[int]string),
	}
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

// Returns true if there are no items available.
func (m *Map) IsEmpty() bool {
	return len(m.keys) == 0
}

// Adds some keys to the hash.
func (m *Map) Add(keys ...string) {
	for _, key := range keys {
		for i := 0; i < m.replicas; i++ {
			hash := int(m.hash([]byte(strconv.Itoa(i) + key)))
			m.keys = append(m.keys, hash)
			m.hashMap[hash] = key
		}
	}
	sort.Ints(m.keys)
}

// Gets the closest item in the hash to the provided key.
func (m *Map) Get(key string) string { //p.peers.Get(key)
	if m.IsEmpty() {
		return ""
	}

	hash := int(m.hash([]byte(key))) // 使用一致性hash函数计算"缓存数据"key的hash值

	// Binary search for appropriate replica. // 选取最小的大于 key的hash值 的 节点hash值
	idx := sort.Search(len(m.keys), func(i int) bool {
		ok := m.keys[i] >= hash
		//return m.keys[i] >= hash
		return ok
	})

	// Means we have cycled back to the first replica.
	if idx == len(m.keys) {
		idx = 0
	}

	return m.hashMap[m.keys[idx]] // 返回节点（url）          在这里一致性hash是用于查找服务器url的，用来定位具体的服务器
}
