package main

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"strings"
)

func main() {
	http.HandleFunc("/upload/", uploadHandle)    // 上传
	http.HandleFunc("/uploaded/", showPicHandle) //显示图片
	err := http.ListenAndServe(":80", nil)
	fmt.Println(err)
}

// 上传图像接口
func uploadHandle(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/html")

	req.ParseForm()
	if req.Method != "POST" {
		w.Write([]byte(html))
	} else {
		// 接收图片
		uploadFile, handle, err := req.FormFile("image")
		errorHandle(err, w)

		// 检查图片后缀
		ext := strings.ToLower(path.Ext(handle.Filename))
		if ext != ".jpg" && ext != ".png" {
			errorHandle(errors.New("只支持jpg/png图片上传"), w)
			return
			//defer os.Exit(2)
		}

		// 保存图片
		path := "E:\\lzb\\golang\\src\\groupcacheNote\\example\\upload\\"
		os.Mkdir(path, 0777)
		saveFile, err := os.OpenFile(path+handle.Filename, os.O_WRONLY|os.O_CREATE, 0666)
		errorHandle(err, w)
		io.Copy(saveFile, uploadFile)

		defer uploadFile.Close()
		defer saveFile.Close()
		// 上传图片成功
		w.Write([]byte("see the pic: <a target='_blank' href='/uploaded/" + handle.Filename + "'>" + handle.Filename + "</a>"))
	}
}

// 显示图片接口
func showPicHandle(w http.ResponseWriter, req *http.Request) {
	file, err := os.Open("." + req.URL.Path)
	errorHandle(err, w)

	defer file.Close()
	buff, err := ioutil.ReadAll(file)
	errorHandle(err, w)
	w.Write(buff)
}

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
