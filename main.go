package main

import (
	"akita/common"
	"akita/db"
	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
	"html/template"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	webAddr = "0.0.0.0:8990"
)

type Template struct {
	templates *template.Template
}

func (t *Template) Render(w io.Writer, name string, data interface{}, c echo.Context) error {
	return t.templates.ExecuteTemplate(w, name, data)
}

// http://127.0.0.1:8990
func WebUI(webAddr string) {
	e := echo.New()
	// static path
	e.Static("/", "static")
	e.HideBanner = true
	// Logger middleware
	e.Use(middleware.Logger())

	// Recover middleware
	e.Use(middleware.Recover())

	// 注册模板
	t := &Template{
		templates: template.Must(template.ParseGlob("views/*.html")),
	}
	e.Renderer = t

	webUIRouter(e)

	//e.Logger.Fatal(e.Start(webAddr))
	err := e.Start(webAddr)
	common.Info.Println(err)
}

// router
func webUIRouter(e *echo.Echo) {
	e.GET("/", webUIIndex)
}

// webUI Index page
func webUIIndex(c echo.Context) error {
	data := make(map[string]interface{})
	return c.Render(http.StatusOK, "index.html", data)
}

func main() {
	interrup := make(chan os.Signal, 1)
	signal.Notify(interrup, os.Interrupt, os.Kill, syscall.SIGEMT)

	go func() {
		db.Sev.Start() // start akita listening
	}()

	if !db.Sev.IsMaster() {
		go func() {
			for {
				db.Sev.DbSync()
				time.Sleep(500 * time.Millisecond) // do sync request every half second
			}
		}()
	}

	// start webUI
	go WebUI(webAddr)

	// 监听中断, 当未有中断时, 主线程在这里阻塞
	select {
	case <-interrup:
		db.Sev.Close() // recycle resources
		signal.Stop(interrup)
	}
}
