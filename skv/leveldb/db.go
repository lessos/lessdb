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

type DB struct {
	ldb *levigo.DB
}

func Open(cfg skv.Config) (*DB, error) {

	var (
		db  = &DB{}
		err error
	)

	opts := levigo.NewOptions()
	opts.SetCreateIfMissing(true)
	opts.SetCache(levigo.NewLRUCache(3 << 30))
	opts.SetFilterPolicy(levigo.NewBloomFilter(10))
	opts.SetCompression(levigo.SnappyCompression)
	opts.SetMaxOpenFiles(500)

	db.ldb, err = levigo.Open(cfg.DataDir+"/0.0", opts)

	if err == nil {
		db.ttl_worker()
		fmt.Println("lessdb/skv.DB opened")
	}

	return db, err
}

func (db *DB) Close() {
	db.ldb.Close()
}
