package main

import (
	"encoding/binary"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"math/rand"
	"time"
)

const (
	KiB = 1024
	MiB = KiB * 1024
	GiB = MiB * 1024

	Size = 4 * GiB
)

func main() {
	var f int
	_, _ = fmt.Scanln(&f)
	db, err := leveldb.OpenFile("test", &opt.Options{
		Filter: filter.NewBloomFilter(10),
	})
	if err != nil {
		panic(err)
	}
	size := 2 * GiB
	n := size / 520
	defer func() { _ = db.Close() }()
	switch f {
	case 0:
		t := time.Now()
		// 4GB
		for i := 0; i < n; i++ {
			key := make([]byte, 8)
			value := make([]byte, 512)
			binary.BigEndian.PutUint64(key, uint64(i))
			rand.Read(value)
			db.Put(key, value, nil)
		}
		fmt.Printf("write use %.2fs\n", time.Since(t).Seconds())
	case 1:
		t := time.Now()
		s := 50 * MiB
		n = s / 520
		fmt.Printf("entry num: %d\n", n)
		for i := 0; i < n; i++ {
			if i%1000 == 0 {
				fmt.Println(i)
			}
			idx := uint64(rand.Int63n(int64(n)))
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, uint64(idx))
			_, err = db.Get(key, nil)
			if err != nil {
				panic(err)
			}
		}
		fmt.Printf("read use %.2fs\n", time.Since(t).Seconds())
	case 2:

	}
}
