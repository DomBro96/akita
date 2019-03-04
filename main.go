package main

import (
	"akita/common"
	"akita/db"
	"fmt"
	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
	"github.com/takama/daemon"
	"html/template"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	name        = "akita"
	description = "Akita: A Simple Key-Value Database"
	webAddr     = "0.0.0.0:8989"
)

var dependencies = []string{"labstack.echo", "takama.daemon"}

type Service struct {
	daemon.Daemon
}

func (service *Service) Manage() (string, error) {
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
	signal.Notify(interrup, os.Interrupt, os.Kill, syscall.SIGEMT)

	go func() { // start akita listening
		db.Sev.Start()
	}()

	if !db.Sev.IsMaster() {
		go func() {
			for {
				db.Sev.DbSync()
				time.Sleep(500 * time.Millisecond) // do sync request every half second
			}
		}()
	}

	select {
	case <-interrup:
		str := "Akita server was stopped. "
		err := db.Sev.Close() // recycle resources
		if err != nil {
			str = "Akita server stop error: %s\n. "
		} else {
			signal.Stop(interrup)
		}
		return str, err
	}
}

// http://127.0.0.1:8989
func (service *Service) WebUI(webAddr string) {
	e := echo.New()
	// static path
	e.Static("/", "static")

	// Logger middleware
	e.Use(middleware.Logger())

	// Recover middleware
	e.Use(middleware.Recover())

	// 注册模板
	t := &Template{
		templates: template.Must(template.ParseGlob("views/*.html")),
	}
	e.Renderer = t

	service.webUIRouter(e)

	e.Logger.Fatal(e.Start(webAddr))
}

// router
func (service *Service) webUIRouter(e *echo.Echo) {
	e.GET("/", service.webUIIndex)
}

// webUI Index page
func (service *Service) webUIIndex(c echo.Context) error {
	data := make(map[string]interface{})

	return c.Render(http.StatusOK, "index.html", data)
}

type Template struct {
	templates *template.Template
}

func (t *Template) Render(w io.Writer, name string, data interface{}, c echo.Context) error {
	return t.templates.ExecuteTemplate(w, name, data)
}

func main() {
	srv, err := daemon.New(name, description, dependencies...)
	if err != nil {
		common.Error.Fatalf("Akita service start error: %s\n", err)
	}
	service := &Service{srv}
	status, err := service.Manage()
	if err != nil {
		common.Error.Fatalf(status, err)
	}

	// start webUI
	go service.WebUI(webAddr)

	fmt.Println(status)
}
