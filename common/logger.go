package common

import (
	"io"
	"log"
	"os"
)

var (
	Info    *log.Logger	// 重要的信息
	Warning *log.Logger // 需要注意的信息
	Error 	*log.Logger // 非常严重的问题
)

func init()  {
	file, err := os.OpenFile("errors.txt", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("Failed to open error log file:", err)	// 失败日志文件读写异常， 退出程序
	}
	Info = log.New(os.Stdout, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
	Warning = log.New(os.Stdout, "WARNING: ", log.Ldate|log.Ltime|log.Lshortfile)
	Error = log.New(io.MultiWriter(file, os.Stderr), "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)	// Error 打印到文件和标准输出
}
