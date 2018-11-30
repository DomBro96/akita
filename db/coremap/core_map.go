package coremap

import "akita/db"

type coreMap struct {
	Map       map[string]int64
	CurOffset int64
}

var MapInstance *coreMap

func SingletonCoreMap() *coreMap {
	if MapInstance == nil {
		MapInstance = &coreMap{}
	}
	return MapInstance
}

func (cm *coreMap) set(record *db.AkitaRecord) (bool, error) {
	return false, nil
}

func (cm *coreMap) get(key string) (int64, error)  {
	return 0, nil
}

func (cm *coreMap) del(key string) (bool, error) {
	return false, nil
}
