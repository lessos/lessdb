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
	"bytes"
	"encoding/binary"
	"strconv"

	"github.com/jmhodges/levigo"
	"github.com/lessos/lessdb/skv"
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

func (db *DB) Zget(key, member []byte) *skv.Reply {
	return db._raw_get(_zset_key(key, member))
}

func (db *DB) Zset(key, member []byte, score uint64) *skv.Reply {

	wb := levigo.NewWriteBatch()
	defer wb.Close()

	// wo := levigo.NewWriteOptions()
	// defer wo.Close()

	//
	if prev := db.Zget(key, member); prev.Status == skv.ReplyOK && prev.Uint64() != score {

		wb.Delete(_zscore_key(key, member, prev.Uint64()))

	} else if prev.Status == skv.ReplyNotFound {
		db._raw_incrby(_zlen_key(key), 1)
	}

	//
	wb.Put(_zscore_key(key, member, score), []byte{})

	//
	wb.Put(_zset_key(key, member), []byte(strconv.FormatUint(score, 10)))

	rpl := skv.NewReply("")

	if err := db.ldb.Write(db.writeOpts, wb); err != nil {
		rpl.Status = err.Error()
	}

	return rpl
}

func (db *DB) Zrange(key []byte, score_start, score_end, limit uint64) *skv.Reply {

	var (
		bs_start = _zscore_key_prefix(key, score_start)
		bs_end   = _zscore_key_prefix(key, score_end)
		rpl      = skv.NewReply("")
	)

	for i := len(bs_end); i < 256; i++ {
		bs_end = append(bs_end, 0xff)
	}

	// ro := levigo.NewReadOptions()
	// ro.SetFillCache(false)
	// defer ro.Close()

	it := db.ldb.NewIterator(db.iteratorReadOpts)
	defer it.Close()

	for it.Seek(bs_start); it.Valid(); it.Next() {

		if limit < 1 {
			break
		}

		if bytes.Compare(it.Key(), bs_end) > 0 {
			break
		}

		if len(it.Key()) < (len(key) + 10) {
			db._raw_del(it.Key())
			continue
		}

		ui64 := binary.BigEndian.Uint64(it.Key()[len(key)+2 : (len(key) + 10)])

		rpl.Data = append(rpl.Data, bytesClone(it.Key()[(len(key)+10):]))
		rpl.Data = append(rpl.Data, bytesClone([]byte(strconv.FormatUint(ui64, 10))))

		limit--
	}

	if err := it.GetError(); err != nil {
		rpl.Status = err.Error()
	}

	return rpl
}

func (db *DB) Zdel(key, member []byte) *skv.Reply {

	wb := levigo.NewWriteBatch()
	defer wb.Close()

	// wo := levigo.NewWriteOptions()
	// defer wo.Close()

	wb.Delete(_zset_key(key, member))

	if prev := db.Zget(key, member); prev.Status == skv.ReplyOK {
		db._raw_incrby(_zlen_key(key), -1)
		wb.Delete(_zscore_key(key, member, prev.Uint64()))
	}

	rpl := skv.NewReply("")

	if err := db.ldb.Write(db.writeOpts, wb); err != nil {
		rpl.Status = err.Error()
	}

	return rpl
}

func (db *DB) Zlen(key []byte) *skv.Reply {
	return db._raw_get(_zlen_key(key))
}
