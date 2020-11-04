package handler

import (
	"akita/ahttp"
	"akita/common"
	"akita/db"
	"akita/logger"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/golang/protobuf/proto"
)

// Save handle insert data request.
func Save(w http.ResponseWriter, req *http.Request) {
	if !db.Eng.IsMaster() {
		ahttp.WriteResponse(w, http.StatusUnauthorized, "sorry this akita node isn't master node! ")
		return
	}
	key := req.FormValue("key")
	if key == "" {
		ahttp.WriteResponse(w, http.StatusBadRequest, "key can not be empty! ")
		return
	}
	if len(common.StringToByteSlice(key)) > 10*common.K {
		ahttp.WriteResponse(w, http.StatusBadRequest, common.ErrKeySize)
		return
	}
	_, file, err := req.FormFile("file")
	if file == nil {
		ahttp.WriteResponse(w, http.StatusBadRequest, "file can not be empty! ")
		return
	}
	if err != nil {
		logger.Error.Printf("Get form file fail: %s\n", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var length int64
	if length = file.Size; length > 64*common.M {
		logger.Error.Printf("Upload file too large: %v\n", length)
		ahttp.WriteResponse(w, http.StatusBadRequest, "file is too large to save. ")
		return
	}
	src, err := file.Open()
	if err != nil {
		logger.Error.Printf("File open fail: %s\n", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer src.Close()
	_, err = db.Eng.Insert(key, src, length)
	if err != nil {
		logger.Error.Printf("File save key %v fail: %v\n", key, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	ahttp.WriteResponse(w, http.StatusOK, "save  key: "+key+" success! ")
}

// Search handle get data request.
func Search(w http.ResponseWriter, req *http.Request) {
	key := req.URL.Query()["key"][0]
	if key == "" {
		ahttp.WriteResponse(w, http.StatusOK, "key can not be empty!  ")
		return
	}
	value, err := db.Eng.Seek(key)
	if err != nil {
		logger.Error.Printf("Seek key %v error %v", key, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	ahttp.WriteResponseWithContextType(w, http.StatusOK, "image/jpeg", value)
}

// Del handle delete data request.
func Del(w http.ResponseWriter, req *http.Request) {
	if !db.Eng.IsMaster() {
		ahttp.WriteResponse(w, http.StatusUnauthorized, "sorry this akita node isn't master node! ")
		return
	}
	key := req.URL.Query()["key"][0]
	if key == "" {
		ahttp.WriteResponse(w, http.StatusOK, "key can not be empty!  ")
		return
	}
	_, delOffset, err := db.Eng.Delete(key)
	if err != nil {
		logger.Error.Printf("Delete key %v fail: %v\n", key, err)
		ahttp.WriteResponse(w, http.StatusInternalServerError, "delete key: "+key+" fail: "+err.Error())
		return
	}
	ahttp.WriteResponse(w, http.StatusOK, delOffset)
}

// Sync deal with slaves sync request.
func Sync(w http.ResponseWriter, req *http.Request) {
	if !db.Eng.IsMaster() {
		ahttp.WriteResponse(w, http.StatusUnauthorized, "sorry this akita node isn't master node! ")
		return
	}
	reqBody := req.Body
	defer reqBody.Close()

	offsetBuf, err := ioutil.ReadAll(reqBody)
	if err != nil {
		logger.Error.Printf("Read http body error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	syncOffset := &db.SyncOffset{}
	err = proto.Unmarshal(offsetBuf, syncOffset)
	if err != nil {
		logger.Error.Printf("proto data unmarshal error: %s \n", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	logger.Info.Printf("request offset is %d\n", syncOffset.Offset)

	complete := make(chan error)
	dataCh := make(chan []byte)
	go func() {
		data, err := db.Eng.GetDB().GetDataByOffset(syncOffset.Offset)
		dataCh <- data
		complete <- err
	}()
	data := <-dataCh
	err = <-complete

	syncData := &db.SyncData{}
	if err != nil {
		if err == common.ErrNoDataUpdate {
			notifier := make(chan struct{})
			db.Eng.Register(req.Host, notifier)
			select {
			case <-time.After(1000 * time.Millisecond):
				syncData.Code = 0
				syncData.Data = nil
			case <-notifier:
				go func() {
					data, err := db.Eng.GetDB().GetDataByOffset(syncOffset.Offset)
					dataCh <- data
					complete <- err
				}()
				data = <-dataCh
				err = <-complete

				logger.Info.Printf("the data length is %d\n", len(data))
				if err != nil {
					logger.Error.Printf("get data by offset error :%s\n", err)
					syncData.Code = 0
					syncData.Data = nil
				}
				syncData.Code = 1
				syncData.Data = data
			}
		} else {
			logger.Error.Printf("get data by offset error :%s\n", err)
			syncData.Code = 0
			syncData.Data = nil
		}
	} else {
		syncData.Code = 1
		syncData.Data = data
		logger.Info.Printf("the data length is %d\n", len(data))
	}
	protoData, _ := proto.Marshal(syncData)
	// use protobuf format to transport data
	ahttp.WriteResponseWithContextType(w, http.StatusOK, "application/protobuf", protoData)
}
