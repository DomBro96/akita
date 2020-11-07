package db

import (
	"akita/common"
	"fmt"
	"path/filepath"
	"testing"
)

func TestDB_WriteRecord(t *testing.T) {
	path := "/users/dombro/akdb/akdata.dat"
	//fileIndex := strings.LastIndex(path, "/")
	//fmt.Println(path[:fileIndex])fileIndex
	dir := filepath.Dir(path)
	ok, err := common.FileIsExit(dir)
	if ok && err == nil {
		fmt.Println("exit")
	} else if !ok && err == nil {
		fmt.Println("not exit")
	}

	fmt.Println(dir)
}

func TestDB_OpenDB(t *testing.T) {
	t.Log("testing...")
	d := OpenDB("/usr/local/akdata/akita.dat")
	t.Logf("db: %v.", d)
	t.Log("testing end.")
}
