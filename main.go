package main

import (
	"akita/common"
	"akita/db"
	"fmt"
	"github.com/takama/daemon"
	"os"
	"os/signal"
	"syscall"
)

const (
	name = "akita"
	description = "Akita: A Simple Key-Value Database"
)

var dependencies = []string{"labstack.echo", "takama.daemon"}

type Service struct {
	daemon.Daemon
}

func (service *Service) Manage() (string, error)  {
	usage := "Usage: akita install | remove | start | stop | status"
	 if len(os.Args) > 1 {
	 	command := os.Args[1]
		 switch command {
		 case "install":
		 		return service.Install()
		 case "remove":
		 		return service.Remove()
		 case "start":
		 		return service.Start()
		 case "stop":
		 		return service.Stop()
		 case "status":
		 		return service.Status()
		 default:
			 	return usage, nil
		 }
	 }
	interrup := make(chan os.Signal, 1)
	// 接收中断信号
	signal.Notify(interrup, os.Interrupt, os.Kill, syscall.SIGEMT)
	// 启动 akita 监听
	go func() {
		db.Sev.Start()
	}()
	select {
	// 接收到中断信号，中断程序
	case <-interrup:
		str := "Akita server was stopped. "
		// 资源的回收
		err := db.Sev.Close()
		signal.Stop(interrup)
		if err != nil {
			str = "Akita server stop error: %s\n. "
		}
		return str, err
	}
}

func main() {
	srv, err := daemon.New(name, description, dependencies...)
	if err != nil {
		common.Error.Fatalf("Akita Service start error: %s\n", err)
	}
	service := &Service{srv}
	status, err := service.Manage()
	if err != nil {
		common.Error.Fatalf(status, err)
	}
	fmt.Println(status)
}
