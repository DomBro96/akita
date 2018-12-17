package db

import (
	"akita/common"
	"fmt"
	"github.com/labstack/echo"
	"mime/multipart"
	"net/http"
	"os"
	"strings"
)

type Server struct {
	master string     // 主库 ip
	slaves []string   // 从库 ip
	dB     *DB        // DB 属性
	echo   *echo.Echo // echo Server 连接
}

var (
	Sev *Server
	port string
	host string
)

func (s *Server) Insert(key string, src multipart.File, length int64) (bool, error) {
	keyBuf := common.StringToByteSlice(key)
	valueBuf := make([]byte, length)
	ks := len(keyBuf)
	vs := len(valueBuf)
	dr := &dataRecord{
		dateHeader: &dataHeader{
			Ks:   int32(ks),
			Vs:   int32(vs),
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
		return false, err
	}
	it := db.iTable
	ri := &recordIndex{offset: offset, size: int(<-lengthChan)}
	it.put(key, ri) // 设置 map 索引
	return true, nil
}

func (s *Server) Seek(key string) ([]byte, error) {
	db := s.dB
	it := s.dB.iTable
	ri := it.get(key) // 获取该记录的起始 offset
	if ri == nil {
		return nil, nil
	}
	fmt.Println(ri.offset)
	fmt.Println(ri.size)
	valueChan := make(chan []byte)
	errChan := make(chan error)
	go func(bc chan []byte, ec chan error) {
		value, err := db.ReadRecord(ri.offset, int64(ri.size))
		bc <- value
		ec <- err
	}(valueChan, errChan)
	if err := <-errChan; err != nil {
		return nil, err
	}
	value, ok := <-valueChan
	fmt.Println(ok)
	close(valueChan)
	close(errChan)
	return value, nil
}

func (s *Server) Delete(key string) (bool, int64, error) { // 删除数据, 返回删除数据的 offset
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
	go func(filePath string, from int64, record *dataRecord) {
		_, err := db.WriteRecord(record)
		errChan <- err
	}(common.DefaultDataFile, db.size, dr)

	if err := <-errChan; err != nil {
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
		fmt.Println(err)
		return ctx.JSON(http.StatusOK, "file upload fail. ")
	}
	var length int64
	if length = file.Size ; length > 10*common.M {
		return ctx.JSON(http.StatusOK, common.ErrFileSize)
	}
	src, err := file.Open()
	defer src.Close()
	if err != nil {
		fmt.Println(1, err)
		return ctx.JSON(http.StatusOK, err)
	}

	_, err = Sev.Insert(key, src, length)
	if err != nil {
		fmt.Println(4, err)
		return ctx.JSON(http.StatusOK, err)
	}
	return ctx.JSON(http.StatusOK, "save success! ")
}

func search(ctx echo.Context) error  {
	key := ctx.QueryParam("key")
	if key == "" {
		return ctx.JSON(http.StatusOK, "key can not be empty!  ")
	}
	value, err := Sev.Seek(key)
	if err != nil {
		fmt.Println(err)
		return ctx.JSON(http.StatusOK, err)
	}
	return ctx.JSON(http.StatusOK, value)
}

func del(ctx echo.Context) error  {
	key := ctx.QueryParam("key")
	if key == "" {
		return ctx.JSON(http.StatusOK, "key can not be empty!  ")
	}
	_, delOffset, err := Sev.Delete(key)
	if err != nil {
		return ctx.JSON(http.StatusOK, err)
	}
	return ctx.JSON(http.StatusOK, delOffset)
}

func (s *Server) Start() error{
	err := s.echo.Start(host  + ":" + port)
	if err != nil {
		fmt.Println(err)
	}
	return err
}


func init() {
	c := new(common.Config)
	file, _ := os.Getwd()
	initFile := file + string(os.PathSeparator) + "conf" + string(os.PathSeparator) + "akita.ini"
	c.InitConfig(initFile)
	slave := c.ConfMap["server.slaves"]
	slave = strings.TrimSpace(slave)
	slave = strings.Replace(slave, "{", "", 1)
	slave = strings.Replace(slave, "}", "", 1)
	slaves := strings.Split(slave, ",")
	Sev = &Server{
		master: c.ConfMap["server.master"],
		slaves: slaves,
		echo: echo.New(),
		dB: OpenDB(c.ConfMap["db.datafile"]),
	}
	port = c.ConfMap["server.port"]
	host = c.ConfMap["server.host"]
	Sev.echo.HideBanner = true
	Sev.echo.POST("/akita/save", save)
	Sev.echo.GET("/akita/search", search)
	Sev.echo.GET("/akita/del", del)
}