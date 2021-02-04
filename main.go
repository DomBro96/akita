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
)

var (
	port                 = flag.String("port", "3664", "akita listening port.")
	master               = flag.String("master_addr", "localhost", "master node ip address. ")
	slaves               = flag.String("slaves_addr", "", "slaves nodes ip address set. ")
	dataFilePath         = flag.String("data_file", "/usr/local/akdata.dat", "akita data file path. ")
	useCache             = flag.Bool("use_cache", true, "use lru cache.")
	cacheLimit           = flag.Int("cache_limit", 1000, "maximum number of caches.")
	dataFileSyncInterval = flag.Int64("dfs_interval", 1000, "data fille synchronization interval, in milliseconds.")
	dbSyncInterval       = flag.Int64("dbs_interval", 500, "db master-slaves synchronization interval, in milliseconds.")
)

func main() {

	http.HandleFunc("/akita/save/", handler.Save)
	http.HandleFunc("/akita/search/", handler.Search)
	http.HandleFunc("/akita/del/", handler.Del)
	http.HandleFunc("/akita/sync/", handler.Sync)

	server := &http.Server{Addr: ":" + *port, Handler: nil}
	db.GetEngine().Start(server, *dataFileSyncInterval, *dbSyncInterval) // start akita listening

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, os.Kill)
	select {
	case <-interrupt:
		db.GetEngine().Close(server) // recycle resources
		signal.Stop(interrupt)
	}
}

func init() {
	db.InitializeEngine(*master, strings.Split(*slaves, ","), *port, *dataFilePath, *useCache, *cacheLimit)
	err := db.GetEngine().GetDB().Reload()
	if err != nil {
		logger.Fatalf("reload data base error: %v", err)
	}
}
