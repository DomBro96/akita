package coremap

type MapFactory struct {
	 	CoreMap *coreMap
}

func (factory *MapFactory) GetCoreMap() *coreMap {
	cm := factory.CoreMap
	if cm == nil {
		cm = new(coreMap)
	}
	return cm
}