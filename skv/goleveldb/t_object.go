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
	// "strconv"
	"fmt"
	"strings"
	"sync"

	"github.com/lessos/lessdb/skv"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var (
	_obj_meta_locker   sync.Mutex
	_obj_event_handler skv.ObjectEventHandler
)

func (db *DB) ObjectEventRegister(ev skv.ObjectEventHandler) {

	_obj_meta_locker.Lock()
	defer _obj_meta_locker.Unlock()

	_obj_event_handler = ev
}

func (db *DB) ObjectGet(path string) *skv.Reply {
	return db._raw_get(skv.ObjectEntryIndex(path))
}

func (db *DB) ObjectSet(path string, value interface{}, ttl uint32) *skv.Reply {

	var (
		meta   skv.ObjectMeta
		bkey   = skv.ObjectEntryIndex(path)
		mkey   = skv.ObjectMetaIndex(path)
		bvalue []byte
	)

	switch value.(type) {

	case []byte:
		bvalue = value.([]byte)

	case map[string]interface{}, struct{}:
		bvalue, _ = skv.JsonEncode(value)

	case string:
		bvalue = []byte(value.(string))

	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		bvalue = []byte(fmt.Sprintf("%d", value))

	case bool:
		if value.(bool) {
			bvalue = []byte{1}
		} else {
			bvalue = []byte{0}
		}

	default:
		return skv.NewReply(skv.ReplyInvalidArgument)
	}

	meta = db._raw_get(mkey).ObjectMeta()

	db._obj_meta_sync(&meta, path, int64(len(bvalue)), ttl)

	return db._raw_set(bkey, append(meta.Export(), bvalue...), 0)
}

func (db *DB) ObjectDel(path string) *skv.Reply {

	bkey := skv.ObjectEntryIndex(path)
	mkey := skv.ObjectMetaIndex(path)
	rpl := skv.NewReply("")

	if rs := db._raw_get(mkey); rs.Status == skv.ReplyOK {

		rpl = db._raw_del(bkey)
		if rpl.Status != skv.ReplyOK {
			return rpl
		}

		ms := rs.ObjectMeta()

		db._obj_meta_sync(&ms, path, -1, 0)

		if _obj_event_handler != nil {
			_obj_event_handler(path, 0, skv.ObjectEventDeleted)
		}
	}

	return rpl
}

func (db *DB) ObjectScan(path, cursor, end string, limit uint32) *skv.Reply {

	if limit > uint32(skv.ScanMaxLimit) {
		limit = uint32(skv.ScanMaxLimit)
	}

	var (
		prefix = skv.ObjectEntryFold(path)
		prelen = len(prefix)
		cstart = append(prefix, skv.ObjectStringHex(cursor)...)
		cend   = append(prefix, skv.ObjectStringHex(end)...)
		rpl    = skv.NewReply("")
	)

	for i := len(cend); i < 256; i++ {
		cend = append(cend, 0xff)
	}

	iter := db.ldb.NewIterator(&util.Range{Start: cstart, Limit: append(cend)}, nil)

	for iter.Next() {

		if limit < 1 {
			break
		}

		if len(iter.Key()) < prelen {
			continue
		}

		if len(iter.Value()) < 53 {
			continue
		}

		rpl.Data = append(rpl.Data, skv.BytesClone(iter.Key()[prelen:]))
		rpl.Data = append(rpl.Data, skv.BytesClone(iter.Value()))

		limit--
	}

	iter.Release()

	if iter.Error() != nil {
		rpl.Status = iter.Error().Error()
	}

	return rpl
}

func (db *DB) ObjectMetaGet(path string) *skv.Reply {
	return db._raw_get(skv.ObjectMetaIndex(path))
}

func (db *DB) ObjectMetaScan(path, cursor, end string, limit uint64) *skv.Reply {

	if limit > skv.ScanMaxLimit {
		limit = skv.ScanMaxLimit
	}

	var (
		prefix = skv.ObjectMetaFold(path)
		prelen = len(prefix)
		cstart = append(prefix, skv.ObjectStringHex(cursor)...)
		cend   = append(prefix, skv.ObjectStringHex(end)...)
		rpl    = skv.NewReply("")
	)

	for i := len(cend); i < 256; i++ {
		cend = append(cend, 0xff)
	}

	iter := db.ldb.NewIterator(&util.Range{Start: cstart, Limit: append(cend)}, nil)

	for iter.Next() {

		if limit < 1 {
			break
		}

		if len(iter.Key()) < prelen {
			continue
		}

		rpl.Data = append(rpl.Data, skv.BytesClone(iter.Key()[prelen:]))
		rpl.Data = append(rpl.Data, skv.BytesClone(iter.Value()))

		limit--
	}

	iter.Release()

	if iter.Error() != nil {
		rpl.Status = iter.Error().Error()
	}

	return rpl
}

func (db *DB) _obj_meta_sync(meta *skv.ObjectMeta, path string, size int64, ttl uint32) string {

	cpath, fold, field, _, _ := skv.ObjectPathSplit(path)

	//
	if meta.Type > 0 {
		if meta.Type != skv.ObjectTypeEntry || meta.Name != field {
			return skv.ReplyInvalidArgument
		}
	}

	//
	if size >= 0 {

		meta.Version = 0

		//
		if meta.Created < 1 {
			meta.Created = skv.MetaTimeNow()
		}
		meta.Updated = skv.MetaTimeNow()

		meta.Len = 0

		meta.Name = field
	}

	_obj_meta_locker.Lock()
	defer _obj_meta_locker.Unlock()

	var (
		fold_size int64
		fold_name string
		fold_meta = db._raw_get(skv.ObjectMetaIndex(fold)).ObjectMeta()
	)

	if fold_meta.Type > 0 && fold_meta.Type != skv.ObjectTypeFold {
		return skv.ReplyInvalidArgument
	}

	if i := strings.LastIndex(fold, "/"); i > 0 {
		fold_name = fold[:i]
	} else {
		fold_name = fold
	}

	if fold_meta.Type > 0 && fold_meta.Name != fold_name {
		return skv.ReplyInvalidArgument
	}

	//
	fold_meta.Type = skv.ObjectTypeFold
	fold_meta.Name = fold_name

	//
	if size >= 0 && meta.Type < 1 {
		fold_meta.Len++
	} else if size < 0 && fold_meta.Len > 0 {
		fold_meta.Len--
	}
	meta.Type = skv.ObjectTypeEntry

	if size >= 0 {
		fold_size = int64(fold_meta.Size) + (size - int64(meta.Size))
		meta.Size = uint64(size)
	} else {
		fold_size = int64(fold_meta.Size) - int64(meta.Size)
	}

	if fold_size > 0 {
		fold_meta.Size = uint64(fold_size)
	} else {
		fold_meta.Size = 0
	}

	//
	if fold_meta.Created < 1 {
		fold_meta.Created = skv.MetaTimeNow()
	}
	fold_meta.Updated = skv.MetaTimeNow()

	//
	batch := new(leveldb.Batch)

	if size >= 0 {

		if ok := db._raw_set_ttl(skv.NsObjectEntry, []byte(path), ttl); !ok {
			return "ServerError TTL Set"
		}

		batch.Put(skv.ObjectMetaIndex(cpath), meta.Export())

	} else {
		batch.Delete(skv.ObjectMetaIndex(cpath))
	}

	if fold_meta.Len < 1 {
		batch.Delete(skv.ObjectMetaIndex(fold))
	} else {
		batch.Put(skv.ObjectMetaIndex(fold), fold_meta.Export())
	}

	if err := db.ldb.Write(batch, nil); err != nil {
		return err.Error()
	}

	return skv.ReplyOK
}
