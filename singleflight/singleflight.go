/*
Copyright 2012 Google Inc.

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

// Package singleflight provides a duplicate function call suppression
// mechanism.
// signleflight 包提供了多函数并发调用的串行化机制
// singleflight处理相同key的多个请求访问磁盘或者内存，只有一个请求访问磁盘或者内存，其他等待结果。
// 并不仅仅是互斥，而是对于某个key资源的访问，如果有人在获取，那么其他的就等着结果，直接用了，防止惊群效应
package singleflight

import "sync"

// call is an in-flight or completed Do call
// call 用来表示一个正在执行或已完成的函数调用
type call struct {
	wg  sync.WaitGroup
	val interface{}
	err error
}

// Group represents a class of work and forms a namespace in which
// units of work can be executed with duplicate suppression.
// Group 可以看做是任务的分类
type Group struct {
	mu sync.Mutex  // 用于锁map的     // protects m
	m  map[string]*call // lazily initialized
}

// Do executes and returns the results of the given function, making
// sure that only one execution is in-flight for a given key at a
// time. If a duplicate comes in, the duplicate caller waits for the
// original to complete and receives the same results.
// 保证只有一个 goroutine 获取key的值，Do返回fn函数的执行结果，多goroutine安全的
func (g *Group) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	g.mu.Lock() // singleflight 用于保护map的读写
	if g.m == nil {
		g.m = make(map[string]*call)
	}
	if c, ok := g.m[key]; ok { // key存在说明有其他请求，所以等着，其他请求完成后，这个请求可以直接获取结果
		g.mu.Unlock()
		c.wg.Wait() // 对于key的多个请求，只有一个请求会触发func()函数,其他就等着
		return c.val, c.err
	}
	c := new(call) // 新建一个call
	c.wg.Add(1)
	g.m[key] = c // 说明这个key已经有一个call在执行了，那么其他请求会感知到，就会等着call执行的结果，并直接使用
	g.mu.Unlock()

	c.val, c.err = fn() // 调用实际的函数
	c.wg.Done()

	g.mu.Lock() // 保护g.m map的读写
	delete(g.m, key) // 删除key
	g.mu.Unlock()

	return c.val, c.err
}
