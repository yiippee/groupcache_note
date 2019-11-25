package main

import (
	"errors"
	"fmt"
	"groupcacheNote"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path"
	"strings"
)

var (
	peers_addrs = []string{"http://127.0.0.1:28001", "http://127.0.0.1:28002", "http://127.0.0.1:28003"}
)

func main() {
	if len(os.Args) != 2 {
		fmt.Println("args: ", os.Args)
		fmt.Println("\r\n Usage local_addr \t\n local_addr must in(127.0.0.1:18001,localhost:18002,127.0.0.1:18003)\r\n")
		os.Exit(1)
	}
	local_addr := os.Args[1] // 设置本地ip地址
	/*
		httppool 是一个集群节点选取器，保存所有节点信息，获取对等节点缓存时，
		通过计算key的“一致性哈希值”与节点的哈希值比较来选取集群中的某个节点
	*/
	peers := groupcache.NewHTTPPool("http://" + local_addr)
	// 将各个机器的ip地址设置到一致性哈希列表中，在这里哈希map是用来找机器的
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
				key = "E:/lzb/golang/src/groupcacheNote/example/" + key
				result, err := ioutil.ReadFile(key) // 访问本地数据库或者文件
				if err != nil {
					fmt.Printf("read file error %s.\n", err.Error())
					return err
				}
				fmt.Printf("asking for %s from local file system\n", key)
				dest.SetBytes([]byte(result))
				return nil
			}))

	// 响应http请求
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

	// 上次图片
	http.HandleFunc("/upload", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "text/html")

		req.ParseForm()
		if req.Method != "POST" {
			w.Write([]byte(html))
		} else {

			pic := "test.png"
			// 检查图片后缀
			ext := strings.ToLower(path.Ext(pic))
			if ext != ".jpg" && ext != ".png" {
				errorHandle(errors.New("只支持jpg/png图片上传"), w)
				return
				//defer os.Exit(2)
			}
			// 一致性hash算法计算图片所在的服务器地址
			key := pic
			if peer, ok := image_cache.GetPeers().PickPeer(key); ok { // 如果根据一致性hash算出来的key在本地，就会返回false，则可直接从本地获取
				// key在其他服务器
				remote, err := url.Parse(peer.GetBaseHost())
				if err != nil {
					panic(err)
				}

				// NewSingleHostReverseProxy 需要池化吗？ 感觉不需要，NewSingleHostReverseProxy也没做多少事
				// 虽然每一次都需要重新创建。但是又感觉可以池化啊
				proxy := httputil.NewSingleHostReverseProxy(remote) // 这个也可以代理websocket？？？感觉是可以upgrade的
				//如果代理出错，则转向其他后端服务，并检查这个出错服务是否正常，如果不正常则踢出iplist
				//proxy.ErrorHandler = func(writer http.ResponseWriter, request *http.Request, e error) {
				//	robin.Del(addr)
				//}
				proxy.ServeHTTP(w, req)
				//value, err := g.getFromPeer(ctx, peer, key) // 构造protobuf数据，向其他节点发起http请求，查找数据，并存储到hotcache
				//if err == nil {
				//	return nil
				//}
			} else {
				// 保存图片
				// 接收图片
				uploadFile, handle, err := req.FormFile("image")
				errorHandle(err, w)

				path := "E:\\lzb\\golang\\src\\groupcacheNote\\example\\upload\\"
				os.Mkdir(path, 0777)
				saveFile, err := os.OpenFile(path+handle.Filename, os.O_WRONLY|os.O_CREATE, 0666)
				errorHandle(err, w)
				io.Copy(saveFile, uploadFile)

				defer uploadFile.Close()
				defer saveFile.Close()
				fmt.Println("save pic successful...")
				// 上传图片成功
				w.Write([]byte("see the pic: <a target='_blank' href='/uploaded/" + handle.Filename + "'>" + handle.Filename + "</a>"))
			}
		}
	})
	log.Fatal(http.ListenAndServe(local_addr, nil))
}

// 上传图像接口

// 统一错误输出接口
func errorHandle(err error, w http.ResponseWriter) {
	if err != nil {
		w.Write([]byte(err.Error()))
	}
}

const html = `<html>
    <head></head>
    <body>
        <form method="post" enctype="multipart/form-data">
            <input type="file" name="image" />
            <input type="submit" />
        </form>
    </body>

</html>`
