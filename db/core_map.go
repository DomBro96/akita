package db


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

func (cm *coreMap) set(record *DataRecord) (bool, error) { // 将记录放入索引

	return false, nil
}

func (cm *coreMap) get(key string) (int64, error)  {
	return 0, nil
}

func (cm *coreMap) del(key string) (bool, error) {
	return false, nil
}
