package db

import (
	"akita/common"
	"sync"
)

type (
		indexRecord struct {
			offset 		int64   // 记录的位置索引
			key         []byte
			size        int32
		}

		akitaMap struct {
			Map       map[string]int64
			CurOffset int64
			AkMutex   sync.RWMutex // 读写锁
		}
)


var mapInstance *akitaMap

// 全局只有一个 akitaMap 的实例, 并且不向外部的包暴露
func getSingletonAkitaMap() *akitaMap {
	if mapInstance == nil {
		mapInstance = &akitaMap{Map: map[string]int64{}, CurOffset: 0,}
	}
	return mapInstance
}

func (cm *akitaMap) set(key string) error { 				// 将记录放入索引
	cm.AkMutex.Lock()
	cm.Map[key] = cm.CurOffset
	defer cm.AkMutex.Unlock()
	return nil
}

func (cm *akitaMap) get(key string) (int64, error)  { 		// 在索引中查找
	cm.AkMutex.RLock()
	value, exists := cm.Map[key]
	if !exists {
		return -1, common.ErrNoSuchRecord
	}
	defer cm.AkMutex.RUnlock()
	return value, nil
}

func (cm *akitaMap) del(key string) (bool, int64, error) {	 // 在索引中删除, 返回删除是否成功以及 value
	cm.AkMutex.Lock()
	value, exists :=  cm.Map[key]
	if exists {
		delete(cm.Map, key)
		return true, value, nil
	}
	defer cm.AkMutex.Unlock()
	return false, -1, common.ErrNoSuchRecord
}
