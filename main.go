package main

import (
	"akita/db"
	"akita/handler"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	port = flag.String("port", "", "akita listening port.")
	master = flag.String("master_addr", "", "master node ip address. ")
	slaves = flag.String("slaves_addr", "", "slaves nodes ip address set. ")

)

func main() {

	http.HandleFunc("/akita/save", handler.Save)
	http.HandleFunc("/akita/search", handler.Search)
	http.HandleFunc("/akita/del", handler.Del)
	http.HandleFunc("/akita/syn", handler.Syn)

	server := &http.Server{Addr: ":"+db.Port, Handler: nil}
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, os.Kill, syscall.SIGEMT)

	go func() {
		db.Eng.Start(server) // start akita listening
	}()

	if !db.Eng.IsMaster() {
		go func() {
			for {
				db.Eng.DbSync()
				time.Sleep(500 * time.Millisecond) // do sync request every half second
			}
		}()
	}

	// 监听中断, 当未有中断时, 主线程在这里阻塞
	select {
	case <-interrupt:
		db.Eng.Close(server) // recycle resources
		signal.Stop(interrupt)
	}
}
