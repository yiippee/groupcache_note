package main

import (
	"fmt"
	"github.com/golang/groupcache"
	"io/ioutil"
	"log"
	"net/http"
	"os"
)

var (
	peers_addrs = []string{"http://127.0.0.1:8001", "http://127.0.0.1:8002", "http://127.0.0.1:8003"}
)

func main() {
	if len(os.Args) != 2 {
		fmt.Println("args: ", os.Args)
		fmt.Println("\r\n Usage local_addr \t\n local_addr must in(127.0.0.1:8001,localhost:8002,127.0.0.1:8003)\r\n")
		os.Exit(1)
	}
	local_addr := os.Args[1]
	/*
		httppool是一个集群节点选取器，保存所有节点信息，获取对等节点缓存时，
		通过计算key的“一致性哈希值”与节点的哈希值比较来选取集群中的某个节点
	*/
	peers := groupcache.NewHTTPPool("http://" + local_addr)
	peers.Set(peers_addrs...)
	// 创建一个group（一个group是一个存储模块，类似命名空间，可以创建多个）
	// NewGroup参数分别是group名字、group大小byte、getter函数
	//（当获取不到key对应的缓存的时候，该函数处理如何获取相应数据，并设置给dest，然后image_cache便会缓存key对应的数据）
	var image_cache = groupcache.NewGroup("image", /*group 的名字，用于分类，比如image file 等*/
		8<<30, groupcache.GetterFunc( /*只需要将普通函数类型转换为 groupcache.GetterFunc 即可，
			因为groupcache.GetterFunc 在定义的时候已经实现了get方法，而get方法会调用下面的函数 */
			// 此函数即为自定义数据处理逻辑。本地处理函数逻辑。
			// 确切地说是访问数据库的处理。因为groupcache会先访问缓存，缓存是分布式的，包括本地的缓存和其他主机上的缓存，
			// 在本地缓存和其他主机上的缓存都没有命中时，才会执行这个访问数据库的函数，执行完会将数据写入本地缓存。
			func(ctx groupcache.Context, key string, dest groupcache.Sink /*sink 类似一个汇聚点，类似网关*/) error {
				key = "C:/Go/gopath/src/github.com/golang/groupcache/example/" + key
				result, err := ioutil.ReadFile(key) // 访问本地数据库或者文件
				if err != nil {
					fmt.Printf("read file error %s.\n", err.Error())
					return err
				}
				fmt.Printf("asking for %s from local file system\n", key)
				dest.SetBytes([]byte(result))
				return nil
			}))
	http.HandleFunc("/image", func(rw http.ResponseWriter, r *http.Request) {
		var data []byte
		k := r.URL.Query().Get("id")
		fmt.Printf("user get %s from groupcache\n", k)
		// group查找对应key的缓存，data需要使用sink（一个数据包装结构）包装一下
		err := image_cache.Get(nil, k, groupcache.AllocatingByteSliceSink(&data)) // get 是分布式的，从分布式的缓存中读取
		if err != nil {
			data = []byte(err.Error())
		}
		rw.Write([]byte(data))
	})
	log.Fatal(http.ListenAndServe(local_addr, nil))
}
