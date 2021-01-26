package handler

import (
	"akita/akerrors"
	"akita/akhttp"
	"akita/common"
	"akita/db"
	"akita/logger"
	"akita/pb"
	"io/ioutil"
	"net/http"
	"time"

	"google.golang.org/protobuf/proto"
)

// Save handle insert data request.
func Save(w http.ResponseWriter, req *http.Request) {
	if !db.GetEngine().IsMaster() {
		akhttp.WriteResponse(w, http.StatusUnauthorized, "sorry this akita node isn't master node! ")
		return
	}
	key := req.FormValue("key")
	if key == "" {
		akhttp.WriteResponse(w, http.StatusBadRequest, "key can not be empty! ")
		return
	}
	if len(common.StringToByteSlice(key)) > 10*common.K {
		akhttp.WriteResponse(w, http.StatusBadRequest, akerrors.ErrKeySize)
		return
	}
	_, file, err := req.FormFile("file")
	if file == nil {
		akhttp.WriteResponse(w, http.StatusBadRequest, "file can not be empty! ")
		return
	}
	if err != nil {
		logger.Errorf("Get form file fail: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var length int64
	if length = file.Size; length > 64*common.M {
		logger.Errorf("Upload file too large: %v", length)
		akhttp.WriteResponse(w, http.StatusBadRequest, "file is too large to save. ")
		return
	}
	src, err := file.Open()
	if err != nil {
		logger.Errorf("File open fail: %s", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer src.Close()
	_, err = db.GetEngine().Insert(key, src, length)
	if err != nil {
		logger.Errorf("File save key %v fail: %v", key, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	akhttp.WriteResponse(w, http.StatusOK, "save  key: "+key+" success! ")
}

// Search handle get data request.
func Search(w http.ResponseWriter, req *http.Request) {
	key := req.URL.Query()["key"][0]
	if key == "" {
		akhttp.WriteResponse(w, http.StatusOK, "key can not be empty!  ")
		return
	}
	value, err := db.GetEngine().Seek(key)
	if err != nil {
		logger.Errorf("Seek key %v error %v", key, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	akhttp.WriteResponseWithContextType(w, http.StatusOK, "image/jpeg", value)
}

// Del handle delete data request.
func Del(w http.ResponseWriter, req *http.Request) {
	if !db.GetEngine().IsMaster() {
		akhttp.WriteResponse(w, http.StatusUnauthorized, "sorry this akita node isn't master node! ")
		return
	}
	key := req.URL.Query()["key"][0]
	if key == "" {
		akhttp.WriteResponse(w, http.StatusOK, "key can not be empty!  ")
		return
	}
	_, delOffset, err := db.GetEngine().Delete(key)
	if err != nil {
		logger.Errorf("Delete key %v fail: %v", key, err)
		akhttp.WriteResponse(w, http.StatusInternalServerError, "delete key: "+key+" fail: "+err.Error())
		return
	}
	akhttp.WriteResponse(w, http.StatusOK, delOffset)
}

// Sync deal with slaves sync request.
func Sync(w http.ResponseWriter, req *http.Request) {
	if !db.GetEngine().IsMaster() {
		akhttp.WriteResponse(w, http.StatusUnauthorized, "sorry this akita node isn't master node! ")
		return
	}
	reqBody := req.Body
	defer reqBody.Close()

	offsetBuf, err := ioutil.ReadAll(reqBody)
	if err != nil {
		logger.Errorf("Read http body error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	syncOffset := &pb.SyncOffset{}
	err = proto.Unmarshal(offsetBuf, syncOffset)
	if err != nil {
		logger.Errorf("proto data unmarshal error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	logger.Infof("request offset is %d", syncOffset.Offset)

	complete := make(chan error)
	dataCh := make(chan []byte)
	go func() {
		data, err := db.GetEngine().GetDB().GetDataByOffset(syncOffset.Offset)
		dataCh <- data
		complete <- err
	}()
	data := <-dataCh
	err = <-complete

	syncData := &pb.SyncData{}
	if err != nil {
		if err == akerrors.ErrNoDataUpdate {
			notifier := make(chan struct{})
			db.GetEngine().Register(req.Host, notifier)
			select {
			case <-time.After(1000 * time.Millisecond):
				syncData.Code = 0
				syncData.Data = nil
			case <-notifier:
				go func() {
					data, err := db.GetEngine().GetDB().GetDataByOffset(syncOffset.Offset)
					dataCh <- data
					complete <- err
				}()
				data = <-dataCh
				err = <-complete

				logger.Infof("the data length is %d", len(data))
				if err != nil {
					logger.Errorf("get data by offset error :%s", err)
					syncData.Code = 0
					syncData.Data = nil
				}
				syncData.Code = 1
				syncData.Data = data
			}
		} else {
			logger.Errorf("get data by offset error :%v", err)
			syncData.Code = 0
			syncData.Data = nil
		}
	} else {
		syncData.Code = 1
		syncData.Data = data
		logger.Infof("the data length is %d", len(data))
	}
	protoData, _ := proto.Marshal(syncData)
	// use protobuf format to transport data
	akhttp.WriteResponseWithContextType(w, http.StatusOK, "application/protobuf", protoData)
}
