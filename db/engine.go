package db

import (
	"akita/akhttp"
	"akita/common"
	"akita/consts"
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

// GetEngine get singletone engine.
func GetEngine() *Engine {
	return engine
}

// InitializeEngine init engine.
func InitializeEngine(master string, slaves []string, port string, dataFilePath string, useCache bool, cacheLimit int) {
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
		logger.Errorf("Insert key %v failed:  %v", key, err)
		return false, err
	}
	ks := len(keyBuf)
	dr := &dataRecord{
		dateHeader: &dataHeader{
			Ks:   int32(ks),
			Vs:   int32(length),
			Flag: consts.FlagWrite,
		},
		key:   keyBuf,
		value: valueBuf,
	}
	db := e.db
	if err := db.WriteRecord(dr); err != nil {
		logger.Errorf("Insert key %v failed:  %v \n", key, err)
		return false, err
	}
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

	value := <-data
	err := <-complete
	if err != nil {
		logger.Errorf("seek key: %v failed. err: %v", key, err)
		return nil, err
	}
	if e.useCache {
		e.cache.insert(key, value)
	}
	return value, nil
}

// Delete delete data from key.
func (e *Engine) Delete(key string) (bool, int64, error) {
	if e.useCache {
		e.cache.remove(key)
	}
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
			Flag: consts.FlagDelete,
		},
		key:   keyBuf,
		value: nil,
	}

	err := e.db.WriteRecordNoCrc32(dr)
	if err != nil {
		logger.Errorf("Delete key: "+key+" failed: %v", err)
		return false, 0, err
	}
	e.notify()
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
		logger.Errorf("marshal data to proto error: %v", err)
		return err
	}
	reader := bytes.NewReader(protoData)
	hc := akhttp.NewHttpClient(2000 * time.Millisecond)
	url := fmt.Sprintf("%v%v:%v%v", "http://", e.master, e.port, "/akita/syn/")
	statusCode, data, err := hc.Post(url, "application/protobuf", reader)
	if err != nil {
		logger.Errorf("sync request fail: %v", err)
		return err
	}
	if statusCode != 200 {
		logger.Infof("sync data from fail info : %v", err)
		return err
	}
	syncData := &pb.SyncData{}
	err = proto.Unmarshal(data, syncData)
	if err != nil {
		logger.Errorf("proto data unmarshal error: %v", err)
		return err
	}
	if syncData.Code != 0 {
		return e.db.WriteSyncData(syncData.Data) // write sync data
	}
	return nil
}

// IsMaster judge server is master or not.
func (e *Engine) IsMaster() bool {
	intranet, err := common.GetIntranetIP()
	if err != nil {
		logger.Fatalln("check your web environment, make sure your machine has intranet ip.")
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
func (e *Engine) Start(server *http.Server, dfsInterval int64, dbsInterval int64) {
	logger.Infoln("akita server starting... ")
	if err := server.ListenAndServe(); err != nil {
		logger.Fatalf("start http server error %v", err)
	}
	go e.db.WriteRecordBuffQueueData()
}

// Close close server, stop provide service.
func (e *Engine) Close(server *http.Server) {
	logger.Infoln("akita server stopping... ")
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		logger.Errorf("shut down http server error %v", err)
		return
	}
	defer e.db.Close()
	logger.Infoln("akita server stopped. ")
}

// TimeExecute execute engine timing tasks, currently hard-coded
// Currently DB.DataFileSync() and DbSync() are executed regularly
func (e *Engine) TimeExecute(dfsInterval int64, dbsInterval int) {
}
