package main

import (
	"akita/db"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, os.Kill, syscall.SIGEMT)

	go func() {
		db.Sever.Start() // start akita listening
	}()

	if !db.Sever.IsMaster() {
		go func() {
			for {
				db.Sever.DbSync()
				time.Sleep(500 * time.Millisecond) // do sync request every half second
			}
		}()
	}

	// 监听中断, 当未有中断时, 主线程在这里阻塞
	select {
	case <-interrupt:
		db.Sever.Close() // recycle resources
		signal.Stop(interrupt)
	}
}
