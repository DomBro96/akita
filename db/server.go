package db

import (
	"akita/common"
	"bytes"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/labstack/echo"
	"mime/multipart"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type Server struct {
	master    string   // master ip
	slaves    []string // slaves ip
	db        *DB
	// TODO: need replace 'echo' with native http server
	echo      *echo.Echo // echo server handle http request
	rwLock    sync.RWMutex
	notifiers map[string]chan struct{} // notifiers notify slaves can get data from
}

var (
	Sever *Server
	port  string
)

func (s *Server) Insert(key string, src multipart.File, length int64) (bool, error) {
	keyBuf := common.StringToByteSlice(key)
	valueBuf := make([]byte, length)
	_, err := src.Read(valueBuf)
	if err != nil {
		common.Error.Printf("Insert key: "+key+" failed:  %s \n", err)
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
	db := s.db
	offset := db.size
	errorChan := make(chan error)
	lengthChan := make(chan int64)
	go func(record *dataRecord) {
		length, err := db.WriteRecord(record)
		errorChan <- err
		lengthChan <- length
	}(dr)

	if err := <-errorChan; err != nil {
		common.Error.Printf("Insert key: "+key+" failed:  %s \n", err)
		return false, err
	}
	it := db.iTable
	ri := &recordIndex{offset: offset, size: int(<-lengthChan)}
	it.put(key, ri)
	s.notify()
	return true, nil
}

func (s *Server) Seek(key string) ([]byte, error) {
	db := s.db
	it := s.db.iTable
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
		return
	}()
	// will block
	value := <-data
	err := <-complete
	if err != nil {
		common.Error.Printf("seek key: %s failed. err: %v \n", key, err)
		return nil, err
	}
	return value, nil
}

func (s *Server) Delete(key string) (bool, int64, error) {
	ri := s.db.iTable.remove(key)
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
		_, err := s.db.WriteRecordNoCrc32(record)
		complete <- err
		return
	}(s.db.size, dr)

	err := <-complete
	if err != nil {
		common.Error.Printf("Delete key: "+key+" failed: %s \n", err)
		return false, 0, err
	}
	s.notify()
	return true, ri.offset, nil
}

func (s *Server) DbSync() error { // sync update data
	s.db.lock.Lock()
	offset := s.db.size
	s.db.lock.Unlock()
	syncOffset := &SyncOffset{
		Offset: offset,
	}
	protoData, err := proto.Marshal(syncOffset)
	if err != nil {
		common.Error.Printf("marshal data to proto error: %s\n", err)
		return err
	}
	reader := bytes.NewReader(protoData)
	hc := common.NewHttpClient(2000 * time.Millisecond)
	url := fmt.Sprintf("%v%v:%v%v", "http://", s.master, port, "/akita/syn")
	statusCode, data, err := hc.Post(url, "application/protobuf", reader)
	if err != nil {
		common.Error.Printf("sync request fail: %s\n", err)
		return err
	}
	if statusCode != 200 {
		common.Info.Printf("sync data from fail info : %s\n", err)
		return err
	}
	syncData := &SyncData{}
	err = proto.Unmarshal(data, syncData)
	if err != nil {
		common.Error.Printf("proto data unmarshal error: %s \n", err)
		return err
	}
	if syncData.Code != 0 {
		complete := make(chan error)
		go func() {
			err := s.db.WriteSyncData(syncData.Data) // write sync data
			complete <- err
		}()
		return <-complete
	}
	return nil
}

func (s *Server) IsMaster() bool { // judge server is master or not
	intranet, err := common.GetIntranetIp()
	if err != nil {
		common.Error.Fatalf("check your web environment， make sure your machine has intranet ip.")
	}
	if intranet == s.master {
		return true
	}
	return false
}

func (s *Server) notify() {
	s.rwLock.Lock()
	for host, notifier := range s.notifiers {
		close(notifier)
		delete(s.notifiers, host)
	}
	s.rwLock.Unlock()
}
func (s *Server) register(slaveHost string, notifier chan struct{}) {
	s.rwLock.Lock()
	s.notifiers[slaveHost] = notifier
	s.rwLock.Unlock()
}
func (s *Server) Start() {
	common.Info.Println("akita server starting... ")
	s.echo.Start(":" + port)
}

func (s *Server) Close() { // close server, stop provide service
	common.Info.Println("akita server stopping... ")
	err := s.db.Close()
	if err != nil {
		common.Error.Printf("akita server stop fail %s\n", err)
		return
	}
	err = s.echo.Close()
	if err != nil {
		common.Error.Fatalf("akita server stop fail %s\n", err)
		return
	}
	common.Info.Println("akita server stopped. ")
}

func init() {
	// TODO: change the configuration from file reading to parameter reading, using flag
	c := new(common.Config)
	file, _ := exec.LookPath(os.Args[0])
	dir := filepath.Dir(file)
	absDir, _ := filepath.Abs(dir)
	initFile := absDir + string(os.PathSeparator) + "conf" + string(os.PathSeparator) + "akita.ini"
	abs, _ := filepath.Abs(initFile)
	c.InitConfig(abs)
	slave := c.ConfMap["server.slaves"]
	slave = strings.TrimSpace(slave)
	slave = strings.Replace(slave, "{", "", 1)
	slave = strings.Replace(slave, "}", "", 1)
	slaves := strings.Split(slave, ",")
	Sever = &Server{
		master:    c.ConfMap["server.master"],
		slaves:    slaves,
		echo:      echo.New(),
		db:        OpenDB(c.ConfMap["db.datafile"]),
		notifiers: make(map[string]chan struct{}),
	}
	errChan := make(chan error)
	go func() {
		err := Sever.db.Reload()
		errChan <- err
	}()
	err := <-errChan
	if err != nil {
		common.Error.Fatalf("Reload data base erro: %s\n", err)
	}
	port = c.ConfMap["server.port"]
	Sever.echo.HideBanner = true
	Sever.echo.POST("/akita/save", save)
	Sever.echo.GET("/akita/search", search)
	Sever.echo.GET("/akita/del", del)
	Sever.echo.POST("/akita/syn", syn)
}
