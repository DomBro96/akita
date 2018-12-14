package db

import (
	"akita/common"
	"github.com/labstack/echo"
	"net/http"
)

type Server struct {
	master string     // 主库 ip
	slaves []string   // 从库 ip
	dB     *DB        // DB 属性
	echo   *echo.Echo // echo Server 连接
}

var (
	Sev *Server
)

func (s *Server) Insert(key string, fileName string) (bool, error) {
	keyBuf := common.StringToByteSlice(key)
	bufLen, err := common.GetFileSize(fileName)
	if err != nil {
		return false, err
	}
	valueBuf, err := common.ReadFileToByte(fileName, 0, bufLen)
	if err != nil {
		return false, err
	}
	ks := len(keyBuf)
	vs := len(valueBuf)
	if ks > common.K {
		return false, common.ErrKeySize
	}
	if vs > 10*common.M {
		return false, common.ErrFileSize
	}
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

	if err = <-errorChan; err != nil {
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
	valueChan := make(chan []byte)
	errChan := make(chan error)
	go func() {
		value, err := db.ReadRecord(ri.offset, int64(ri.size))
		valueChan <- value
		errChan <- err
	}()
	if err := <-errChan; err != nil {
		return nil, err
	}
	value := <-valueChan
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
		return ctx.JSON(http.StatusOK, "key can not be empty!  ")
	}
	file, err := ctx.FormFile("file")

	if file == nil {
		return ctx.JSON(http.StatusOK, "file can not be empty! ")
	}

	if err != nil {
		return ctx.JSON(http.StatusInternalServerError, "file upload fail. ")
	}
	if file.Size > 10*common.M {
		return ctx.JSON(http.StatusOK, common.ErrFileSize)
	}
	_, err = Sev.Insert(key, file.Filename)
	if err != nil {
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
	return s.echo.Start(":8989")
}


func init() {
	Sev = &Server{
		echo: echo.New(),
		dB: OpenDB(),
	}
	Sev.echo.POST("/akita/save", save)
	Sev.echo.GET("/akita/search", search)
	Sev.echo.GET("/akita/del", del)
}