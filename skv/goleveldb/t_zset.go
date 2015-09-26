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
	"encoding/binary"
	"strconv"

	"github.com/lessos/lessdb/skv"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func (db *DB) Zget(key, member []byte) *skv.Reply {
	return db._raw_get(skv.ZsetKey(key, member))
}

func (db *DB) Zset(key, member []byte, score uint64) *skv.Reply {

	batch := new(leveldb.Batch)

	//
	if prev := db.Zget(key, member); prev.Status == skv.ReplyOK && prev.Uint64() != score {

		batch.Delete(skv.ZsetScoreKey(key, member, prev.Uint64()))

	} else if prev.Status == skv.ReplyNotFound {
		db._raw_incrby(skv.ZsetLenKey(key), 1)
	}

	//
	batch.Put(skv.ZsetScoreKey(key, member, score), []byte{})

	//
	batch.Put(skv.ZsetKey(key, member), []byte(strconv.FormatUint(score, 10)))

	rpl := skv.NewReply("")

	if err := db.ldb.Write(batch, nil); err != nil {
		rpl.Status = err.Error()
	}

	return rpl
}

func (db *DB) Zrange(key []byte, score_start, score_end, limit uint64) *skv.Reply {

	var (
		bs_start = skv.ZsetScoreKeyPrefix(key, score_start)
		bs_end   = skv.ZsetScoreKeyPrefix(key, score_end)
		rpl      = skv.NewReply("")
	)

	for i := len(bs_end); i < 256; i++ {
		bs_end = append(bs_end, 0xff)
	}

	iter := db.ldb.NewIterator(&util.Range{Start: bs_start, Limit: bs_end}, nil)

	for iter.Next() {

		if limit < 1 {
			break
		}

		if len(iter.Key()) < (len(key) + 10) {
			db._raw_del(iter.Key())
			continue
		}

		ui64 := binary.BigEndian.Uint64(iter.Key()[len(key)+2 : (len(key) + 10)])

		rpl.Data = append(rpl.Data, skv.BytesClone(iter.Key()[(len(key)+10):]))
		rpl.Data = append(rpl.Data, skv.BytesClone([]byte(strconv.FormatUint(ui64, 10))))

		limit--
	}

	iter.Release()

	if iter.Error() != nil {
		rpl.Status = iter.Error().Error()
	}

	return rpl
}

func (db *DB) Zdel(key, member []byte) *skv.Reply {

	batch := new(leveldb.Batch)

	batch.Delete(skv.ZsetKey(key, member))

	if prev := db.Zget(key, member); prev.Status == skv.ReplyOK {
		db._raw_incrby(skv.ZsetLenKey(key), -1)
		batch.Delete(skv.ZsetScoreKey(key, member, prev.Uint64()))
	}

	rpl := skv.NewReply("")

	if err := db.ldb.Write(batch, nil); err != nil {
		rpl.Status = err.Error()
	}

	return rpl
}

func (db *DB) Zlen(key []byte) *skv.Reply {
	return db._raw_get(skv.ZsetLenKey(key))
}
