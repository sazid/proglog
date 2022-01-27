package server

import (
	"fmt"
	"sync"
)

type Log struct {
	mu      *sync.Mutex
	records []Record
}

func NewLog() *Log {
	return &Log{
		mu:      new(sync.Mutex),
		records: make([]Record, 0),
	}
}

func (c *Log) Append(record Record) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	record.Offset = uint64(len(c.records))
	fmt.Println("Offset: ", record.Offset)
	c.records = append(c.records, record)
	return record.Offset, nil
}

func (c *Log) Read(offset uint64) (Record, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if offset >= uint64(len(c.records)) {
		return Record{}, ErrOffsetNotFound
	}
	return c.records[offset], nil
}

type Record struct {
	Value  []byte `json:"value"`
	Offset uint64 `json:"offset"`
}

var ErrOffsetNotFound = fmt.Errorf("offset not found")
