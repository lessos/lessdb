// Copyright 2015 lessOS.com, All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package leveldb

import (
	"fmt"

	"github.com/jmhodges/levigo"
	"github.com/lessos/lessdb/skv"
)

const (
	KiB = 1024
	MiB = KiB * 1024
	GiB = MiB * 1024
)

type DB struct {
	ldb              *levigo.DB
	readOpts         *levigo.ReadOptions
	writeOpts        *levigo.WriteOptions
	iteratorReadOpts *levigo.ReadOptions
}

func Open(cfg skv.Config) (*DB, error) {

	var (
		db  = &DB{}
		err error
	)

	opts := levigo.NewOptions()
	opts.SetCreateIfMissing(true)
	opts.SetCache(levigo.NewLRUCache(cfg.CacheCapacity * MiB))
	opts.SetWriteBufferSize(cfg.WriteBuffer * MiB)
	opts.SetBlockSize(4 * KiB)
	opts.SetFilterPolicy(levigo.NewBloomFilter(10))
	opts.SetCompression(levigo.SnappyCompression)
	opts.SetMaxOpenFiles(500)

	// fmt.Println("cache", cfg.CacheCapacity, "WriteBuffer", cfg.WriteBuffer)

	db.ldb, err = levigo.Open(cfg.DataDir+"/0.0", opts)

	//
	db.readOpts = levigo.NewReadOptions()
	db.readOpts.SetFillCache(true)

	//
	db.writeOpts = levigo.NewWriteOptions()

	//

	db.iteratorReadOpts = levigo.NewReadOptions()
	db.iteratorReadOpts.SetFillCache(false)

	//
	if err == nil {
		db.ttl_worker()
		fmt.Println("lessdb/skv.DB opened")
	}

	return db, err
}

func (db *DB) Close() {

	db.iteratorReadOpts.Close()
	db.writeOpts.Close()
	db.readOpts.Close()

	db.ldb.Close()
}
