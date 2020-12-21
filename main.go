package main

import (
	"akita/db"
	"akita/handler"
	"akita/logger"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"
)

var (
	port         = flag.String("port", "3664", "akita listening port.")
	master       = flag.String("master_addr", "localhost", "master node ip address. ")
	slaves       = flag.String("slaves_addr", "", "slaves nodes ip address set. ")
	dataFilePath = flag.String("data_file", "/usr/local/akdata.dat", "akita data file path. ")
	useCache     = flag.Bool("use_cache", false, "use lru cache.")
	cacheLimit   = flag.Int("cache_limit", 1000, "maximum number of caches.")
)

func main() {

	http.HandleFunc("/akita/save/", handler.Save)
	http.HandleFunc("/akita/search/", handler.Search)
	http.HandleFunc("/akita/del/", handler.Del)
	http.HandleFunc("/akita/sync/", handler.Sync)

	server := &http.Server{Addr: ":" + *port, Handler: nil}
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, os.Kill)

	go func() {
		db.DefaultEngine().Start(server) // start akita listening
	}()

	if !db.DefaultEngine().IsMaster() {
		go func() {
			for {
				db.DefaultEngine().DbSync()
				time.Sleep(500 * time.Millisecond) // do sync request every half second
			}
		}()
	}

	select {
	case <-interrupt:
		db.DefaultEngine().Close(server) // recycle resources
		signal.Stop(interrupt)
	}
}

func init() {
	db.InitializeDefaultEngine(*master, strings.Split(*slaves, ","), *port, *dataFilePath, *useCache, *cacheLimit)
	errch := make(chan error)
	go func() {
		err := db.DefaultEngine().GetDB().Reload()
		errch <- err
	}()
	err := <-errch
	if err != nil {
		logger.Error.Fatalf("Reload data base erro: %s\n", err)
	}
	go db.DefaultEngine().GetDB().WriteFromRecordQueue()
}
