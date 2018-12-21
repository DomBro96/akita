package db

import (
	"akita/common"
	"github.com/labstack/echo"
	"mime/multipart"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
)

type Server struct {
	master string     // 主库 ip
	slaves []string   // 从库 ip
	dB     *DB        // DB 属性
	echo   *echo.Echo // echo Server 连接
}

var (
	Sev  *Server
	port string
	host string
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
	db := s.dB
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
	it.put(key, ri) // 设置 map 索引
	return true, nil
}

func (s *Server) Seek(key string) ([]byte, error) {
	var wg sync.WaitGroup
	wg.Add(1)
	db := s.dB
	it := s.dB.iTable
	ri := it.get(key) // 获取该记录的起始 offset
	if ri == nil {
		return nil, nil
	}
	valueChan := make(chan []byte)
	errChan := make(chan error)
	go func(bc chan []byte, ec chan error) {
		defer wg.Done()
		value, err := db.ReadRecord(ri.offset, int64(ri.size))
		bc <- value
		ec <- err
		return
	}(valueChan, errChan)
	value := <-valueChan
	err := <-errChan
	wg.Wait()
	if err != nil {
		common.Error.Printf("Seek key: "+key+" failed:  %s \n", err)
		return nil, err
	}
	return value, nil
}

func (s *Server) Delete(key string) (bool, int64, error) { // 删除数据, 返回删除数据的 offset
	var wg sync.WaitGroup
	wg.Add(1)
	db := s.dB
	it := db.iTable
	ri := it.remove(key)
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
	errChan := make(chan error)
	go func(from int64, record *dataRecord) {
		defer wg.Done()
		_, err := db.WriteRecord(record)
		errChan <- err
		return
	}(db.size, dr)
	err := <-errChan
	wg.Wait()
	if err != nil {
		common.Error.Printf("Delete key: "+key+" failed: %s \n", err)
		return false, 0, err
	}
	return true, ri.offset, nil
}

func save(ctx echo.Context) error {
	key := ctx.FormValue("key")
	if key == "" {
		return ctx.JSON(http.StatusOK, "key can not be empty! ")
	}
	if len(common.StringToByteSlice(key)) > 10*common.K {
		return ctx.JSON(http.StatusOK, common.ErrKeySize)
	}
	file, err := ctx.FormFile("file")
	if file == nil {
		return ctx.JSON(http.StatusOK, "file can not be empty! ")
	}
	if err != nil {
		common.Error.Printf("Get form file fail: %s\n", err)
		return ctx.JSON(http.StatusOK, "file upload fail. Please try again. ")
	}
	var length int64
	if length = file.Size; length > 10*common.M {
		return ctx.JSON(http.StatusOK, "file is too large to save. ")
	}
	src, err := file.Open()
	defer src.Close()
	if err != nil {
		common.Error.Printf("File open fail: %s\n", err)
		return ctx.JSON(http.StatusOK, err)
	}
	_, err = Sev.Insert(key, src, length)
	if err != nil {
		return ctx.JSON(http.StatusOK, "save key: "+key+" fail")
	}
	return ctx.JSON(http.StatusOK, "save  key: "+key+" success! ")
}

func search(ctx echo.Context) error {
	key := ctx.QueryParam("key")
	if key == "" {
		return ctx.JSON(http.StatusOK, "key can not be empty!  ")
	}
	value, err := Sev.Seek(key)
	if err != nil {
		return ctx.JSON(http.StatusOK, "seek key: "+key+" fail. ")
	}
	return ctx.JSON(http.StatusOK, value)
}

func del(ctx echo.Context) error {
	key := ctx.QueryParam("key")
	if key == "" {
		return ctx.JSON(http.StatusOK, "key can not be empty!  ")
	}
	_, delOffset, err := Sev.Delete(key)
	if err != nil {
		return ctx.JSON(http.StatusOK, "delete key: "+key+"fail. ")
	}
	return ctx.JSON(http.StatusOK, delOffset)
}


func (s *Server) Start() error {
	err := s.echo.Start(host + ":" + port)
	if err != nil {
		common.Error.Printf("Akita server start fail : %s\n", err)
	}
	common.Info.Printf("Akita server started. ")
	return err
}

// 关闭数据库服务
func (s *Server) Close() error  {
	// 关闭对外服务
	err := s.echo.Close()
	if err != nil {
		return err
	}
	// 关闭文件写入
	err = s.dB.Close()
	return err
}

func init() {
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
	Sev = &Server{
		master: c.ConfMap["server.master"],
		slaves: slaves,
		echo:   echo.New(),
		dB:     OpenDB(c.ConfMap["db.datafile"]),
	}
	errChan := make(chan error)
	go func() {
		err := Sev.dB.Reload()
		errChan <- err
	}()
	err := <-errChan
	if err != nil {
		common.Error.Fatalf("Reload data base erro: %s\n", err)
	}
	port = c.ConfMap["server.port"]
	host = c.ConfMap["server.host"]
	Sev.echo.HideBanner = true
	Sev.echo.POST("/akita/save", save)
	Sev.echo.GET("/akita/search", search)
	Sev.echo.GET("/akita/del", del)
}
