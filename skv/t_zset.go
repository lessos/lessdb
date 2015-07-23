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
	"encoding/binary"
	"strconv"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func _zset_key(key, member []byte) []byte {
	return append(_raw_key_encode(ns_zset_entry, key), member...)
}

func _zscore_key_prefix(key []byte, score uint64) []byte {

	bscore := make([]byte, 8)
	binary.BigEndian.PutUint64(bscore, score)

	return append(_raw_key_encode(ns_zset_score, key), bscore...)
}

func _zscore_key(key, member []byte, score uint64) []byte {
	return append(_zscore_key_prefix(key, score), member...)
}

func _zlen_key(key []byte) []byte {
	return _raw_key_encode(ns_zset_length, key)
}

func (db *DB) Zget(key, member []byte) *Reply {
	return db._raw_get(_zset_key(key, member))
}

func (db *DB) Zset(key, member []byte, score uint64) *Reply {

	batch := new(leveldb.Batch)

	//
	if prev := db.Zget(key, member); prev.Status == ReplyOK && prev.Uint64() != score {

		batch.Delete(_zscore_key(key, member, prev.Uint64()))

	} else if prev.Status == ReplyNotFound {
		db._raw_incr(_zlen_key(key), 1)
	}

	//
	batch.Put(_zscore_key(key, member, score), []byte{})

	//
	batch.Put(_zset_key(key, member), []byte(strconv.FormatUint(score, 10)))

	rpl := NewReply("")

	if err := db.ldb.Write(batch, nil); err != nil {
		rpl.Status = err.Error()
	}

	return rpl
}

func (db *DB) Zrange(key []byte, score_start, score_end, limit uint64) *Reply {

	var (
		bs_start = _zscore_key_prefix(key, score_start)
		bs_end   = _zscore_key_prefix(key, score_end)
		rpl      = NewReply("")
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

		rpl.Data = append(rpl.Data, bytesClone(iter.Key()[(len(key)+10):]))
		rpl.Data = append(rpl.Data, bytesClone([]byte(strconv.FormatUint(ui64, 10))))

		limit--
	}

	iter.Release()

	if iter.Error() != nil {
		rpl.Status = iter.Error().Error()
	}

	return rpl
}

func (db *DB) Zdel(key, member []byte) *Reply {

	batch := new(leveldb.Batch)

	batch.Delete(_zset_key(key, member))

	if prev := db.Zget(key, member); prev.Status == ReplyOK {
		db._raw_incr(_zlen_key(key), -1)
		batch.Delete(_zscore_key(key, member, prev.Uint64()))
	}

	rpl := NewReply("")

	if err := db.ldb.Write(batch, nil); err != nil {
		rpl.Status = err.Error()
	}

	return rpl
}

func (db *DB) Zlen(key []byte) *Reply {
	return db._raw_get(_zlen_key(key))
}
