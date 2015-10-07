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
	"time"

	"github.com/lessos/lessdb/skv"
	"github.com/syndtr/goleveldb/leveldb"
)

func (db *DB) ttl_worker() {

	go func() {

		for {

			ls := db._raw_ssttl_range(0, skv.TimeNowMS(), skv.TtlWorkerLimit).Hash()

			for _, v := range ls {

				batch := new(leveldb.Batch)

				if db._raw_get(skv.RawTtlEntry(v.Key[9:])).Uint64() == v.Uint64() {

					batch.Delete(skv.RawTtlEntry(v.Key[9:]))

					switch v.Key[9] {

					case skv.NsObjectEntry:
						db.ObjectDel(string(v.Key[10:]))

					case skv.NsKvEntry:
						batch.Delete(v.Key[9:])
					}
				}

				batch.Delete(v.Key)

				db.ldb.Write(batch, nil)
			}

			if uint64(len(ls)) < skv.TtlWorkerLimit {
				time.Sleep(skv.TtlWorkerSleep)
			}
		}
	}()
}
