package coremap

type coreMap struct {
	Map       map[string]int64
	CurOffset int64
}

var MapInstance *coreMap

func SingleCoreMap() *coreMap {
	if MapInstance == nil {
		MapInstance = &coreMap{}
	}
	return MapInstance
}
