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

// Package groupcache provides a data loading mechanism with caching
// and de-duplication that works across a set of peer processes.
//
// Each data Get first consults its local cache, otherwise delegates
// to the requested key's canonical owner, which then checks its cache
// or finally gets the data.  In the common case, many concurrent
// cache misses across a set of peers for the same key result in just
// one cache fill.
package groupcache

import (
	"errors"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"

	pb "groupcacheNote/groupcachepb"
	"groupcacheNote/lru"
	"groupcacheNote/singleflight"
)

// A Getter loads data for a key.
type Getter interface {
	// Get returns the value identified by key, populating dest.
	//
	// The returned data must be unversioned. That is, key must
	// uniquely describe the loaded data, without an implicit
	// current time, and without relying on cache expiration
	// mechanisms.
	Get(ctx Context, key string, dest Sink) error
}

// A GetterFunc implements Getter with a function.
type GetterFunc func(ctx Context, key string, dest Sink) error

func (f GetterFunc) Get(ctx Context, key string, dest Sink) error {
	return f(ctx, key, dest)
}

var (
	mu     sync.RWMutex
	groups = make(map[string]*Group)

	initPeerServerOnce sync.Once
	initPeerServer     func()
)

// GetGroup returns the named group previously created with NewGroup, or
// nil if there's no such group.
func GetGroup(name string) *Group {
	mu.RLock()
	g := groups[name]
	mu.RUnlock()
	return g
}

// NewGroup creates a coordinated group-aware Getter from a Getter.
//
// The returned Getter tries (but does not guarantee) to run only one
// Get call at once for a given key across an entire set of peer
// processes. Concurrent callers both in the local process and in
// other processes receive copies of the answer once the original Get
// completes.
//
// The group name must be unique for each getter.
func NewGroup(name string, cacheBytes int64, getter Getter) *Group {
	return newGroup(name, cacheBytes, getter, nil)
}

// If peers is nil, the peerPicker is called via a sync.Once to initialize it.
func newGroup(name string, cacheBytes int64, getter Getter, peers PeerPicker) *Group {
	if getter == nil {
		panic("nil Getter")
	}
	mu.Lock()
	defer mu.Unlock()
	initPeerServerOnce.Do(callInitPeerServer)
	if _, dup := groups[name]; dup {
		panic("duplicate registration of group " + name)
	}
	g := &Group{ //maincache、hotcache、peerPick都是在函数调用过程中赋值或初始化的
		name:       name,
		getter:     getter,
		peers:      peers,
		cacheBytes: cacheBytes,
		loadGroup:  &singleflight.Group{},
	}
	if fn := newGroupHook; fn != nil {
		fn(g) //此处函数空
	}
	groups[name] = g //保存创建的group
	return g
}

// newGroupHook, if non-nil, is called right after a new group is created.
var newGroupHook func(*Group)

// RegisterNewGroupHook registers a hook that is run each time
// a group is created.
func RegisterNewGroupHook(fn func(*Group)) {
	if newGroupHook != nil {
		panic("RegisterNewGroupHook called more than once")
	}
	newGroupHook = fn
}

// RegisterServerStart registers a hook that is run when the first
// group is created.
func RegisterServerStart(fn func()) {
	if initPeerServer != nil {
		panic("RegisterServerStart called more than once")
	}
	initPeerServer = fn
}

func callInitPeerServer() {
	if initPeerServer != nil {
		initPeerServer()
	}
}

// A Group is a cache namespace and associated data loaded spread over
// a group of 1 or more machines.
// group的作用是管理缓存，并通过httppool选取节点，获取其他节点的缓存
type Group struct {
	name       string //group 名字
	getter     Getter //本地数据获取函数。getter 当缓存中不存在对应数据时，使用该函数获取数据并缓存
	peersOnce  sync.Once
	peers      PeerPicker //http实现了该接口，使用 func (p *HTTPPool) PickPeer(key string) (ProtoGetter, bool) 函数选取节点
	cacheBytes int64      //缓存最大空间 byte // limit for sum of mainCache and hotCache size

	// mainCache is a cache of the keys for which this process
	// (amongst its peers) is authoritative. That is, this cache
	// contains keys which consistent hash on to this process's
	// peer number.
	mainCache cache // 使用lru策略实现的缓存结构，也是key hash值在本地的缓存

	// hotCache contains keys/values for which this peer is not
	// authoritative (otherwise they would be in mainCache), but
	// are popular enough to warrant mirroring in this process to
	// avoid going over the network to fetch from a peer.  Having
	// a hotCache avoids network hotspotting, where a peer's
	// network card could become the bottleneck on a popular key.
	// This cache is used sparingly to maximize the total number
	// of key/value pairs that can be stored globally.
	hotCache cache //使用lru策略实现的缓存结构，key hash值不在本地，作为热点缓存，负载均衡

	// loadGroup ensures that each key is only fetched once
	// (either locally or remotely), regardless of the number of
	// concurrent callers.
	//使用该结构保证当缓存中不存在key对应的数据时，
	// 只有一个goroutine 调用getter函数取数据，
	// 其他正在并发的goroutine会等待直到第一个goroutine返回数据，然后大家一起返回数据
	// 避免惊群现象
	loadGroup flightGroup

	_ int32 // force Stats to be 8-byte aligned on 32-bit platforms 对齐

	// Stats are statistics on the group.
	Stats Stats //统计信息
}

// flightGroup is defined as an interface which flightgroup.Group
// satisfies.  We define this so that we may test with an alternate
// implementation.
type flightGroup interface {
	// Done is called when Do is done.
	Do(key string, fn func() (interface{}, error)) (interface{}, error)
}

// Stats are per-group statistics.
// 所有统计信息都是原子形式操作
type Stats struct {
	Sets           AtomicInt
	Gets           AtomicInt // any Get request, including from peers
	CacheHits      AtomicInt // either cache was good
	PeerLoads      AtomicInt // either remote load or remote cache hit (not an error)
	PeerErrors     AtomicInt
	Loads          AtomicInt // (gets - cacheHits)
	LoadsDeduped   AtomicInt // after singleflight
	LocalLoads     AtomicInt // total good local loads
	LocalLoadErrs  AtomicInt // total bad local loads
	ServerRequests AtomicInt // gets that came over the network from peers
}

// Name returns the name of the group.
func (g *Group) Name() string {
	return g.name
}

func (g *Group) initPeers() {
	if g.peers == nil {
		g.peers = getPeers(g.name)
	}
}

func (g *Group) GetPeers() PeerPicker {
	g.peersOnce.Do(g.initPeers) // 把httppool赋值给 groupcache.PeerPicker
	return g.peers
}

// 上传图片到目标服务器
//func (g *Group) Set(ctx Context, key string, dest Sink) error {
//	g.peersOnce.Do(g.initPeers) // 把httppool赋值给 groupcache.PeerPicker
//	g.Stats.Sets.Add(1)         // 统计信息
//	if dest == nil {
//		return errors.New("groupcache: nil dest Sink")
//	}
//
//	//if peer, ok := g.peers.PickPeer(key); ok { // 如果根据一致性hash算出来的key在本地，就会返回false，则可直接从本地获取
//	//	// key在其他服务器
//	//	value, err := g.getFromPeer(ctx, peer, key) // 构造protobuf数据，向其他节点发起http请求，查找数据，并存储到hotcache
//	//	if err == nil {
//	//		return nil
//	//	}
//	//	g.Stats.PeerErrors.Add(1)
//	//	// TODO(bradfitz): log the peer's error? keep
//	//	// log of the past few for /groupcachez?  It's
//	//	// probably boring (normal task movement), so not
//	//	// worth logging I imagine.
//	//}
//
//	// 在本地，则直接存储
//
//}
//group查找
func (g *Group) Get(ctx Context, key string, dest Sink) error {
	g.peersOnce.Do(g.initPeers) // 把httppool赋值给 groupcache.PeerPicker
	g.Stats.Gets.Add(1)         // 统计信息
	if dest == nil {
		return errors.New("groupcache: nil dest Sink")
	}
	value, cacheHit := g.lookupCache(key) // 从maincache（本机缓存）、hotcache（其他机器数据在本机的缓存）查找

	if cacheHit {
		g.Stats.CacheHits.Add(1)
		return setSinkView(dest, value)
	}

	// Optimization to avoid double unmarshalling or copying: keep
	// track of whether the dest was already populated. One caller
	// (if local) will set this; the losers will not. The common
	// case will likely be one caller.
	destPopulated := false
	// 如果本地缓存和其他节点在本地的热点数据缓存都没有，则需要从其他节点获取数据。因为数据可能在其他节点的缓存中
	// 应该是从后端获取数据了，不一定是其他机器，需要根据key计算一致性hash来定位具体的服务器
	value, destPopulated, err := g.load(ctx, key, dest) // 从对等节点或自定义查找逻辑（getter）中获取数据。缓存是分布式的
	if err != nil {
		return err
	}
	if destPopulated {
		return nil
	}
	return setSinkView(dest, value) //把数据设置给sink
}

// load loads key either by invoking the getter locally or by sending it to another machine.
// 从对等节点或自定义查找逻辑（getter）中获取数据
func (g *Group) load(ctx Context, key string, dest Sink) (value ByteView, destPopulated bool, err error) {
	g.Stats.Loads.Add(1)
	// 用于向其他节点发送查询请求时，合并相同key的请求，减少热点可能带来的麻烦,
	// 比如说我请求key="123"的数据，在没有返回的时候又有很多相同key的请求，
	// 而此时后面的没有必要发，只要等待第一次返回的结果即可.
	// 此函数使用 flightGroup 执行策略，保证只有一个goroutine 调用getter函数取数据。   重复抑制
	viewi, err := g.loadGroup.Do(key, func() (interface{}, error) {
		// Check the cache again because singleflight can only dedup calls
		// that overlap concurrently.  It's possible for 2 concurrent
		// requests to miss the cache, resulting in 2 load() calls.  An
		// unfortunate goroutine scheduling would result in this callback
		// being run twice, serially.  If we don't check the cache again,
		// cache.nbytes would be incremented below even though there will
		// be only one entry for this key.
		//
		// Consider the following serialized event ordering for two
		// goroutines in which this callback gets called twice for hte
		// same key:
		// 1: Get("key")
		// 1: Get("key") //展示了一个有可能2个以上的goroutine同时执行进入了load，
		// 这样会导致同一个key对应的数据被多次获取并统计，所以又执行了一次g.lookupCache(key)
		// 2: Get("key")
		// 1: lookupCache("key")
		// 2: lookupCache("key")
		// 1: load("key")
		// 2: load("key")
		// 1: loadGroup.Do("key", fn)
		// 1: fn()
		// 2: loadGroup.Do("key", fn)
		// 2: fn()
		// 再一次检测缓存，理由如上
		if value, cacheHit := g.lookupCache(key); cacheHit { //通过一致性hash获取对等节点，与httppool对应
			g.Stats.CacheHits.Add(1)
			return value, nil
		}
		g.Stats.LoadsDeduped.Add(1)
		var value ByteView
		var err error
		if peer, ok := g.peers.PickPeer(key); ok { // 如果根据一致性hash算出来的key在本地也会返回false，则可直接从本地加载，而不是去其他服务器了，这样能维持hash一致性
			// key存储在其他服务器
			value, err = g.getFromPeer(ctx, peer, key) // 构造protobuf数据，向其他节点发起http请求，查找数据，并存储到hotcache
			if err == nil {
				g.Stats.PeerLoads.Add(1)
				return value, nil
			}
			g.Stats.PeerErrors.Add(1)
			// TODO(bradfitz): log the peer's error? keep
			// log of the past few for /groupcachez?  It's
			// probably boring (normal task movement), so not
			// worth logging I imagine.
		}
		// 2019-11-25 新的注释以下逻辑是从本地数据库获取数据。这块逻辑只会在key通过一致性hash得出的服务器上执行，其他机器不会执行。这也是用户定义缓存不命中后的处理

		// 为什么不是先从本地取呢？？ 这里之前理解错了
		// 其实前面已经从本地的缓存中尝试过读取，但是均失败，所以需要去其他节点读取缓存。因为从其他节点的内存中读取数据，还是比本地访问数据库快的。
		// 其实想想，一个来自外网的数据请求可能经过了很多个路由器转发才到达这里，那么在内网的服务器之间转发消息其实很快很快了。

		// 如果所有节点中的缓存都没有命中，则需要从数据库中读取了，前面的http请求也是想尝试去读取对等节点的缓存的。
		value, err = g.getLocally(ctx, key, dest) // 从本地获取，调用getter函数获取数据，并存储到maincache。
		// 也不是说从本地获取，而是缓存没命中时，如何处理的问题，只是访问到这台机器，则这台机器去访问全局的DB加载数据。在这里缓存才是分布式的。
		if err != nil {
			g.Stats.LocalLoadErrs.Add(1)
			return nil, err
		}
		g.Stats.LocalLoads.Add(1)
		destPopulated = true                      // only one caller of load gets this return value
		g.populateCache(key, value, &g.mainCache) // 将读取到的value存入本地的mainCache中
		return value, nil
	})
	if err == nil {
		value = viewi.(ByteView)
	}
	return
}

func (g *Group) getLocally(ctx Context, key string, dest Sink) (ByteView, error) {
	err := g.getter.Get(ctx, key, dest)
	if err != nil {
		return ByteView{}, err
	}
	return dest.view()
}

func (g *Group) getFromPeer(ctx Context, peer ProtoGetter, key string) (ByteView, error) {
	req := &pb.GetRequest{
		Group: &g.name,
		Key:   &key,
	}
	res := &pb.GetResponse{}
	err := peer.Get(ctx, req, res)
	if err != nil {
		return ByteView{}, err
	}
	value := ByteView{b: res.Value}
	// TODO(bradfitz): use res.MinuteQps or something smart to
	// conditionally populate hotCache.  For now just do it some
	// percentage of the time.
	if rand.Intn(10) == 0 {
		// 从远程节点获取的数据，有10%的概率会缓存在本地的热点缓存中
		g.populateCache(key, value, &g.hotCache)
	}
	return value, nil
}

// 从maincache、hotcache查找，cache底层使用链表实现并使用lru策略修改链表
func (g *Group) lookupCache(key string) (value ByteView, ok bool) {
	if g.cacheBytes <= 0 {
		return
	}
	value, ok = g.mainCache.get(key) // 从本地的缓存中读取
	if ok {
		return
	}
	value, ok = g.hotCache.get(key) // 其他节点在本地的缓存
	return
}

// 将缓存数据存储在本地的hotcache中，这就会导致热数据会遍布在所有的机器中
func (g *Group) populateCache(key string, value ByteView, cache *cache) {
	if g.cacheBytes <= 0 {
		return
	}
	cache.add(key, value)

	// Evict 去除的意思 items from cache(s) if necessary.
	// 如果缓存大于某个值，需要去除最久未使用的缓存值。控制缓存的容量
	for {
		mainBytes := g.mainCache.bytes()
		hotBytes := g.hotCache.bytes()
		if mainBytes+hotBytes <= g.cacheBytes {
			return
		}

		// TODO(bradfitz): this is good-enough-for-now logic.
		// It should be something based on measurements and/or
		// respecting the costs of different resources.
		victim := &g.mainCache
		if hotBytes > mainBytes/8 {
			victim = &g.hotCache // 如果热点缓存大于本地缓存的1/8 则从热点缓存中去除
		}
		victim.removeOldest() // 删除最久未使用的
	}
}

// CacheType represents a type of cache.
type CacheType int

const (
	// The MainCache is the cache for items that this peer is the
	// owner for.
	MainCache CacheType = iota + 1

	// The HotCache is the cache for items that seem popular
	// enough to replicate to this node, even though it's not the
	// owner.
	HotCache
)

// CacheStats returns stats about the provided cache within the group.
func (g *Group) CacheStats(which CacheType) CacheStats {
	switch which {
	case MainCache:
		return g.mainCache.stats()
	case HotCache:
		return g.hotCache.stats()
	default:
		return CacheStats{}
	}
}

// cache is a wrapper around an *lru.Cache that adds synchronization,
// makes values always be ByteView, and counts the size of all keys and
// values.
type cache struct {
	mu         sync.RWMutex
	nbytes     int64 // of all keys and values
	lru        *lru.Cache
	nhit, nget int64
	nevict     int64 // number of evictions
}

func (c *cache) stats() CacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return CacheStats{
		Bytes:     c.nbytes,
		Items:     c.itemsLocked(),
		Gets:      c.nget,
		Hits:      c.nhit,
		Evictions: c.nevict,
	}
}

func (c *cache) add(key string, value ByteView) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru == nil {
		c.lru = &lru.Cache{
			OnEvicted: func(key lru.Key, value interface{}) {
				val := value.(ByteView)
				c.nbytes -= int64(len(key.(string))) + int64(val.Len())
				c.nevict++
			},
		}
	}
	c.lru.Add(key, value)
	c.nbytes += int64(len(key)) + int64(value.Len())
}

func (c *cache) get(key string) (value ByteView, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nget++
	if c.lru == nil {
		return
	}
	vi, ok := c.lru.Get(key)
	if !ok {
		return
	}
	c.nhit++
	return vi.(ByteView), true
}

func (c *cache) removeOldest() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru != nil {
		c.lru.RemoveOldest()
	}
}

func (c *cache) bytes() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.nbytes
}

func (c *cache) items() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.itemsLocked()
}

func (c *cache) itemsLocked() int64 {
	if c.lru == nil {
		return 0
	}
	return int64(c.lru.Len())
}

// An AtomicInt is an int64 to be accessed atomically.
type AtomicInt int64

// Add atomically adds n to i.
func (i *AtomicInt) Add(n int64) {
	atomic.AddInt64((*int64)(i), n)
}

// Get atomically gets the value of i.
func (i *AtomicInt) Get() int64 {
	return atomic.LoadInt64((*int64)(i))
}

func (i *AtomicInt) String() string {
	return strconv.FormatInt(i.Get(), 10)
}

// CacheStats are returned by stats accessors on Group.
type CacheStats struct {
	Bytes     int64
	Items     int64
	Gets      int64
	Hits      int64
	Evictions int64
}
