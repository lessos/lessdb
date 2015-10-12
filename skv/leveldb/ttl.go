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
	"time"

	"github.com/jmhodges/levigo"
	"github.com/lessos/lessdb/skv"
)

const (
	_ttl_worker_sleep        = 200e6
	_ttl_worker_limit uint64 = 10000
)

func (db *DB) ttl_worker() {

	go func() {

		for {

			ls := db.SsRange(skv.SetTtlPrefix(), 0, skv.TimeNowMS(), _ttl_worker_limit).Hash()

			for _, v := range ls {

				wb := levigo.NewWriteBatch()
				// wo := levigo.NewWriteOptions()

				wb.Delete(skv.SortSetsNsScoreKey(skv.SetTtlPrefix(), v.Key, v.Uint64()))

				if rs := db.SsGet(skv.SetTtlPrefix(), v.Key).Uint64(); rs == v.Uint64() {
					wb.Delete(skv.SortSetsNsEntryKey(skv.SetTtlPrefix(), v.Key))
					wb.Delete(v.Key)
				}

				db.ldb.Write(db.writeOpts, wb)
				wb.Close()
				// wo.Close()

				db._raw_incrby(skv.SortSetsNsLengthKey(skv.SetTtlPrefix()), -1)
			}

			if uint64(len(ls)) < _ttl_worker_limit {
				time.Sleep(_ttl_worker_sleep)
			}
		}
	}()
}
