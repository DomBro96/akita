package db

import "akita/common"

type akitaMap struct {
	Map       map[string]int64
	CurOffset int64
}

var mapInstance *akitaMap

// 全局只有一个 akitaMap 的实例
func SingletonAkitaMap() *akitaMap {
	if mapInstance == nil {
		mapInstance = &akitaMap{Map: map[string]int64{}, CurOffset: 0,}
	}
	return mapInstance
}

func (cm *akitaMap) set(key string) error { // 将记录放入索引
	cm.Map[key] = cm.CurOffset
	return nil
}

func (cm *akitaMap) get(key string) (int64, error)  { // 在索引中查找
	value, exists := cm.Map[key]
	if !exists {
		return -1, common.ErrNoSuchRecord
	}
	return value, nil
}

func (cm *akitaMap) del(key string) (bool, error) {
	return false, nil
}
