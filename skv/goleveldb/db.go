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

package goleveldb

import (
	"fmt"
	"os"

	"github.com/lessos/lessdb/skv"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type DB struct {
	ldb *leveldb.DB
}

func Open(cfg skv.Config) (*DB, error) {

	var (
		db  = &DB{}
		err error
	)

	cfg.ReFix()

	os.MkdirAll(cfg.DataDir+cfg.DataDirGroup, 0750)

	db.ldb, err = leveldb.OpenFile(cfg.DataDir+cfg.DataDirGroup, &opt.Options{
		WriteBuffer:            cfg.WriteBuffer * opt.MiB,
		BlockCacheCapacity:     cfg.BlockCacheCapacity * opt.MiB,
		OpenFilesCacheCapacity: cfg.OpenFilesCacheCapacity,
		CompactionTableSize:    cfg.CompactionTableSize * opt.MiB,
		Compression:            opt.SnappyCompression,
		Filter:                 filter.NewBloomFilter(10),
	})

	if err == nil {
		db.ttl_worker()
		fmt.Println("lessdb/skv.DB " + cfg.DataDir + " opened")
	}

	return db, err
}

func (db *DB) Close() {
	db.ldb.Close()
}
