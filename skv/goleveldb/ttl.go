// Copyright 2015-2016 lessdb Author, All rights reserved.
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
	"time"

	"github.com/lessos/lessdb/dbutil"
	"github.com/lessos/lessdb/skv"
	"github.com/syndtr/goleveldb/leveldb"
)

const (
	_ttl_worker_sleep        = 500e6
	_ttl_worker_limit uint64 = 10000
)

func (db *DB) ttl_worker() {

	go func() {

		for {

			ls := db._raw_ssttlat_range(0, dbutil.MetaTimeNow(), _ttl_worker_limit).Hash()

			for _, v := range ls {

				batch := new(leveldb.Batch)

				if dbutil.BytesToUint64(db.RawGet(skv.RawTtlEntry(v.Key[9:])).Bytes()) == v.Uint64() {

					batch.Delete(skv.RawTtlEntry(v.Key[9:]))

					switch v.Key[9] {

					case skv.NsObjectEntry:
						db.ObjectDel(string(v.Key[10:]))

					case skv.NsKvEntry:
						batch.Delete(v.Key[9:])
					}
				}
				// fmt.Println("ttl  delete", dbutil.BytesToUint64(v.Key[1:9]), v.Key[10:])

				batch.Delete(v.Key)

				db.ldb.Write(batch, nil)
			}

			if uint64(len(ls)) < _ttl_worker_limit {
				time.Sleep(_ttl_worker_sleep)
			}
		}
	}()
}
