package main

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"math/rand"
	"os"
)

func main() {
	os.RemoveAll("testdb")
	o := &opt.Options{
		Filter: filter.NewBloomFilter(10),
	}
	db, err := leveldb.OpenFile("testdb", o)
	if err != nil {
		panic(err)
	}
	go func() {
		for {
			key := make([]byte, 20)
			rand.Read(key)
			db.Get(key, nil)
		}
	}()
	for {
		key, value := make([]byte, 20), make([]byte, 128)
		rand.Read(key)
		rand.Read(value)
		db.Put(key, value, nil)
	}
}
