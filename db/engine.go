package db

import (
	"akita/ahttp"
	"akita/common"
	"akita/logger"
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
	master string   // master ip
	slaves []string // slaves ips
	port   string
	db     *DB
	sync.RWMutex
	notifiers map[string]chan struct{} // notifiers notify slaves can get data from
}

var (
	engine *Engine
)

// DefaultEngine get singletone engine.
func DefaultEngine() *Engine {
	return engine
}

// InitializeDefaultEngine init engine.
func InitializeDefaultEngine(master string, slaves []string, port string, dataFilePath string) {
	engine = &Engine{
		master:    master,
		slaves:    slaves,
		port:      port,
		db:        OpenDB(dataFilePath),
		notifiers: make(map[string]chan struct{}),
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
	ri := &recordIndex{offset: <-offsetChan, size: int(<-lengthChan)}
	it.put(key, ri)
	e.notify()
	return true, nil
}

// Seek get data from key.
func (e *Engine) Seek(key string) ([]byte, error) {
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
	return true, ri.offset, nil
}

// DbSync slaves server update data.
func (e *Engine) DbSync() error {

	offset := e.db.GetSyncSize()
	syncOffset := &SyncOffset{
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
	syncData := &SyncData{}
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
	intranet, err := common.GetIntranetIp()
	if err != nil {
		logger.Error.Fatalf("check your web environment， make sure your machine has intranet ip.")
	}
	if intranet == e.master {
		return true
	}
	return false
}

func (e *Engine) notify() {
	e.Lock()
	for host, notifier := range e.notifiers {
		close(notifier)
		delete(e.notifiers, host)
	}
	e.Unlock()
}

// Register regist slaves to master.
func (e *Engine) Register(slaveHost string, notifier chan struct{}) {
	e.Lock()
	e.notifiers[slaveHost] = notifier
	e.Unlock()
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
	return
}

// func init() {
// 	// TODO: change the configuration from file reading to parameter reading, using flag
// 	c := new(common.Config)
// 	file, _ := exec.LookPath(os.Args[0])
// 	dir := filepath.Dir(file)
// 	absDir, _ := filepath.Abs(dir)
// 	initFile := absDir + string(os.PathSeparator) + "conf" + string(os.PathSeparator) + "akita.ini"
// 	abs, _ := filepath.Abs(initFile)
// 	c.InitConfig(abs)
// 	slave := c.ConfMap["server.slaves"]
// 	slave = strings.TrimSpace(slave)
// 	slave = strings.Replace(slave, "{", "", 1)
// 	slave = strings.Replace(slave, "}", "", 1)
// 	slaves := strings.Split(slave, ",")
// 	InitializeDefaultEngine(c.ConfMap["server.master"], slaves, c.ConfMap["db.datafile"])
// 	errChan := make(chan error)
// 	go func() {
// 		err := DefaultEngine().db.Reload()
// 		errChan <- err
// 	}()
// 	err := <-errChan
// 	if err != nil {
// 		logger.Error.Fatalf("Reload data base erro: %s\n", err)
// 	}
// 	Port = c.ConfMap["server.Port"]
// }
