package wal

import (
	"akita/logger"
	"akita/memtable"
	"os"
	"sync"
)

// WAL represents the write ahead log to restore the state of memtable
type WAL struct {
	sync.Mutex
	walFile *os.File
	enable  bool
	state   int
	memTyp  int
}

func OpenWAL(wfp string) *WAL {
	wf, err := os.OpenFile(wfp, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		logger.Fatalf("open wal file error: %v", err)
	}
	return &WAL{
		walFile: wf,
		enable:  true,
		state:   0,
		memTyp:  0,
	}
}

func (w *WAL) Flush(key string, value []byte) error {
	if !w.enable {
		return nil
	}
	return nil
}

func (w *WAL) RestoreMemtableState() memtable.Memtable {
	return nil
}

func (w *WAL) State() int {
	return w.state
}

func (w *WAL) MemTyp() int {
	return w.memTyp
}
