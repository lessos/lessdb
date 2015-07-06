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

package skv

import (
	"time"

	"github.com/syndtr/goleveldb/leveldb"
)

func (db *DB) ttl_worker() {

	go func() {

		for {

			ls := db.Zrange(ns_set_ttl, 0, timeNowMS(), ttl_job_limit).Hash()

			for _, v := range ls {

				batch := new(leveldb.Batch)

				batch.Delete(v.Key)
				batch.Delete(_zset_key(ns_set_ttl, v.Key))
				batch.Delete(_zscore_key(ns_set_ttl, v.Key, v.Uint64()))

				db.ldb.Write(batch, nil)

				db._raw_incr(_zlen_key(ns_set_ttl), -1)
			}

			if uint64(len(ls)) < ttl_job_limit {
				time.Sleep(ttl_job_sleep)
			}
		}
	}()
}
