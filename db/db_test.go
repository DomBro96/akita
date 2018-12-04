package db

import (
	"akita/common"
	"fmt"
	"testing"
)

func TestSingletonCoreMap(t *testing.T) {
	akMap := SingletonCoreMap()
	akMap.Map["dombro"]   = 12
	akMap.Map["dombro96"] = 21
	akMap0 := SingletonCoreMap()
	for key, value := range akMap0.Map {
		fmt.Printf("Key: %s Value: %d\n", key, value)
	}
}

func TestDataRecord_WriteRecord(t *testing.T) {
	record := DataRecord{}
	ok, err := record.WriteRecord(common.DefaultDataFile, 0)
	fmt.Println(ok, err)
	t.Log(err)
	if ok != true {
		t.Error(err)
	}
}
