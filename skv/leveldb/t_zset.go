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

func (db *DB) SsGet(key, member []byte) *skv.Reply {
	return db._raw_get(skv.SortSetsKey(key, member))
}

func (db *DB) SortSets(key, member []byte, score uint64) *skv.Reply {

	wb := levigo.NewWriteBatch()
	defer wb.Close()

	// wo := levigo.NewWriteOptions()
	// defer wo.Close()

	//
	if prev := db.SsGet(key, member); prev.Status == skv.ReplyOK && prev.Uint64() != score {

		wb.Delete(skv.SortSetsScoreKey(key, member, prev.Uint64()))

	} else if prev.Status == skv.ReplyNotFound {
		db._raw_incrby(skv.SortSetsLenKey(key), 1)
	}

	//
	wb.Put(skv.SortSetsScoreKey(key, member, score), []byte{})

	//
	wb.Put(skv.SortSetsKey(key, member), []byte(strconv.FormatUint(score, 10)))

	rpl := skv.NewReply("")

	if err := db.ldb.Write(db.writeOpts, wb); err != nil {
		rpl.Status = err.Error()
	}

	return rpl
}

func (db *DB) SsRange(key []byte, score_start, score_end, limit uint64) *skv.Reply {

	var (
		bs_start = skv.SortSetsScoreKeyPrefix(key, score_start)
		bs_end   = skv.SortSetsScoreKeyPrefix(key, score_end)
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

		rpl.Data = append(rpl.Data, skv.BytesClone(it.Key()[(len(key)+10):]))
		rpl.Data = append(rpl.Data, skv.BytesClone([]byte(strconv.FormatUint(ui64, 10))))

		limit--
	}

	if err := it.GetError(); err != nil {
		rpl.Status = err.Error()
	}

	return rpl
}

func (db *DB) SsDel(key, member []byte) *skv.Reply {

	wb := levigo.NewWriteBatch()
	defer wb.Close()

	// wo := levigo.NewWriteOptions()
	// defer wo.Close()

	wb.Delete(skv.SortSetsKey(key, member))

	if prev := db.SsGet(key, member); prev.Status == skv.ReplyOK {
		db._raw_incrby(skv.SortSetsLenKey(key), -1)
		wb.Delete(skv.SortSetsScoreKey(key, member, prev.Uint64()))
	}

	rpl := skv.NewReply("")

	if err := db.ldb.Write(db.writeOpts, wb); err != nil {
		rpl.Status = err.Error()
	}

	return rpl
}

func (db *DB) SsLen(key []byte) *skv.Reply {
	return db._raw_get(skv.SortSetsLenKey(key))
}
