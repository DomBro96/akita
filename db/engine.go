package db

import (
	"akita/ahttp"
	"akita/common"
	"akita/logger"
	"akita/pb"
	"bytes"
	"context"
	"fmt"
	"mime/multipart"
	"net/http"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
)

// Engine kv database engine.
type Engine struct {
	sync.RWMutex
	master    string   // master ip
	slaves    []string // slaves ips
	port      string
	db        *DB
	notifiers map[string]chan struct{} // notifiers notify slaves can get data from
	useCache  bool
	cache     *hashTableLRUCache
}

var (
	engine *Engine
)

// DefaultEngine get singletone engine.
func DefaultEngine() *Engine {
	return engine
}

// InitializeDefaultEngine init engine.
func InitializeDefaultEngine(master string, slaves []string, port string, dataFilePath string, useCache bool, cacheLimit int) {
	engine = &Engine{
		master:    master,
		slaves:    slaves,
		port:      port,
		db:        OpenDB(dataFilePath),
		notifiers: make(map[string]chan struct{}),
		useCache:  useCache,
	}
	if useCache {
		engine.cache = newHashTableLRUCache(cacheLimit)
	}
}

// GetDB get engine db.
func (e *Engine) GetDB() *DB {
	return e.db
}

// Insert insert binary data to databae.
func (e *Engine) Insert(key string, src multipart.File, length int64) (bool, error) {
	keyBuf := common.StringToByteSlice(key)
	valueBuf := make([]byte, length)
	_, err := src.Read(valueBuf)
	if err != nil {
		logger.Error.Printf("Insert key %v failed:  %v \n", key, err)
		return false, err
	}
	ks := len(keyBuf)
	dr := &dataRecord{
		dateHeader: &dataHeader{
			Ks:   int32(ks),
			Vs:   int32(length),
			Flag: common.WriteFlag,
		},
		key:   keyBuf,
		value: valueBuf,
	}
	db := e.db
	errorChan := make(chan error)
	offsetChan := make(chan int64)
	lengthChan := make(chan int64)
	go func(record *dataRecord) {
		offset, length, err := db.WriteRecord(record)
		offsetChan <- offset
		lengthChan <- length
		errorChan <- err
	}(dr)

	if err := <-errorChan; err != nil {
		logger.Error.Printf("Insert key: "+key+" failed:  %v \n", err)
		return false, err
	}
	it := db.iTable
	ri := &recordIndex{offset: <-offsetChan, size: <-lengthChan}
	it.put(key, ri)
	e.notify()
	if e.useCache {
		e.cache.insert(key, valueBuf)
	}
	return true, nil
}

// Seek get data from key.
func (e *Engine) Seek(key string) ([]byte, error) {
	if e.useCache {
		cn := e.cache.search(key)
		if cn != nil {
			return cn.data, nil
		}
	}
	db := e.db
	it := e.db.iTable
	ri := it.get(key)
	if ri == nil {
		return nil, nil
	}
	data := make(chan []byte)
	complete := make(chan error)
	go func() {
		value, err := db.ReadRecord(ri.offset, int64(ri.size))
		data <- value
		complete <- err
	}()
	// will block
	value := <-data
	err := <-complete
	if err != nil {
		logger.Error.Printf("seek key: %v failed. err: %v \n", key, err)
		return nil, err
	}
	if e.useCache {
		e.cache.insert(key, value)
	}
	return value, nil
}

// Delete delete data from key.
func (e *Engine) Delete(key string) (bool, int64, error) {
	ri := e.db.iTable.remove(key)
	if ri == nil {
		return false, 0, nil
	}
	keyBuf := common.StringToByteSlice(key)
	ks := len(keyBuf)
	dr := &dataRecord{
		dateHeader: &dataHeader{
			Ks:   int32(ks),
			Vs:   int32(0),
			Flag: common.DeleteFlag,
		},
		key:   keyBuf,
		value: nil,
	}
	complete := make(chan error)
	go func(from int64, record *dataRecord) {
		_, err := e.db.WriteRecordNoCrc32(record)
		complete <- err
	}(e.db.size, dr)

	err := <-complete
	if err != nil {
		logger.Error.Printf("Delete key: "+key+" failed: %v \n", err)
		return false, 0, err
	}
	e.notify()
	if e.useCache {
		e.cache.remove(key)
	}
	return true, ri.offset, nil
}

// DbSync slaves server update data.
func (e *Engine) DbSync() error {

	offset := e.db.GetSyncSize()
	syncOffset := &pb.SyncOffset{
		Offset: offset,
	}
	protoData, err := proto.Marshal(syncOffset)
	if err != nil {
		logger.Error.Printf("marshal data to proto error: %v\n", err)
		return err
	}
	reader := bytes.NewReader(protoData)
	hc := ahttp.NewHttpClient(2000 * time.Millisecond)
	url := fmt.Sprintf("%v%v:%v%v", "http://", e.master, e.port, "/akita/syn/")
	statusCode, data, err := hc.Post(url, "application/protobuf", reader)
	if err != nil {
		logger.Error.Printf("sync request fail: %v\n", err)
		return err
	}
	if statusCode != 200 {
		logger.Info.Printf("sync data from fail info : %v\n", err)
		return err
	}
	syncData := &pb.SyncData{}
	err = proto.Unmarshal(data, syncData)
	if err != nil {
		logger.Error.Printf("proto data unmarshal error: %v \n", err)
		return err
	}
	if syncData.Code != 0 {
		complete := make(chan error)
		go func() {
			err := e.db.WriteSyncData(syncData.Data) // write sync data
			complete <- err
		}()
		return <-complete
	}
	return nil
}

// IsMaster judge server is master or not.
func (e *Engine) IsMaster() bool {
	intranet, err := common.GetIntranetIP()
	if err != nil {
		logger.Warning.Panicln("check your web environment， make sure your machine has intranet ip.")
	}
	return intranet == e.master
}

func (e *Engine) notify() {
	e.Lock()
	defer e.Unlock()
	for host, notifier := range e.notifiers {
		close(notifier)
		delete(e.notifiers, host)
	}
}

// Register regist slaves to master.
func (e *Engine) Register(slaveHost string, notifier chan struct{}) {
	e.Lock()
	defer e.Unlock()
	e.notifiers[slaveHost] = notifier
}

// Start start akita server service.
func (e *Engine) Start(server *http.Server) {
	logger.Info.Println("akita server starting... ")
	if err := server.ListenAndServe(); err != nil {
		logger.Error.Fatalf("start http server error %v", err)
	}
}

// Close close server, stop provide service.
func (e *Engine) Close(server *http.Server) {

	logger.Info.Println("akita server stopping... ")
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		logger.Error.Printf("shut down http server error %v", err)
		return
	}

	err := e.db.Close()
	if err != nil {
		logger.Error.Printf("akita server stop fail %v\n", err)
		return
	}
	logger.Info.Println("akita server stopped. ")
}
