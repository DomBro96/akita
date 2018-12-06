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
	 conn := &Connection{}
	 ok, err := conn.Insert("akita", "/Users/dombro/go/src/akita/file_test/akita_src.jpg")
	 if ok {
	 	fmt.Println("insert success. ")
	 }
	 if err != nil {
	 	fmt.Println(err)
	 }
	 size0, err := common.GetFileSize(common.DefaultDataFile)
	 if err != nil {
	 	fmt.Println(err)
	 }
	 fmt.Println(size0)
	value, err := conn.Seek("akita")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(SingletonCoreMap().CurOffset)
	size, err := common.WriteFileWithByte("/Users/dombro/go/src/akita/file_test/akita_copy.jpg", 0, value)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(size)
}

func TestConnection_Seek(t *testing.T) {
	conn := &Connection{}
	value, err := conn.Seek("akita")
	if err != nil {
		fmt.Println(err)
	}
	size, err := common.WriteFileWithByte("/Users/dombro/go/src/akita/file_test/akita_copy.jpg", 0, value)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(size)
}