package db

import "akita/common"

type coreMap struct {
	Map       map[string]int64
	CurOffset int64
}

var mapInstance *coreMap

// 全局只有一个 coreMap 的实例
func SingletonCoreMap() *coreMap {
	if mapInstance == nil {
		mapInstance = &coreMap{Map: map[string]int64{}, CurOffset: 0,}
	}
	return mapInstance
}

func (cm *coreMap) set(key string) error { 				// 将记录放入索引
	cm.Map[key] = cm.CurOffset
	return nil
}

func (cm *coreMap) get(key string) (int64, error)  {	// 在索引中查找
	value, exists := cm.Map[key]
	if !exists {
		return -1, common.ErrNoSuchRecord
	}
	return value, nil
}

func (cm *coreMap) del(key string) (bool, error) {
	return false, nil
}
