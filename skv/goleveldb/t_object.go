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

func (db *DB) ObjectSet(path string, value []byte, ttl uint32) *skv.Reply {

	bkey := skv.ObjectEntryIndex(path)

	var prev_num int32
	var prev_size int64

	if rs := db._raw_get(bkey); rs.Status == skv.ReplyNotFound {
		prev_num, prev_size = 1, int64(len(value))
	} else if rs.Status == skv.ReplyOK {
		prev_size = int64(len(value) - len(rs.Bytes()))
	}

	db._obj_meta_sync(path, 0, prev_num, prev_size, int64(len(value)), ttl)

	return db._raw_set(bkey, value, 0)
}

func (db *DB) ObjectDel(path string) *skv.Reply {

	bkey := skv.ObjectEntryIndex(path)
	rpl := skv.NewReply("")

	if rs := db._raw_get(bkey); rs.Status == skv.ReplyOK {
		rpl = db._raw_del(bkey)
		db._obj_meta_sync(path, 0, -1, int64(-len(rs.Bytes())), 0, 0)

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

func (db *DB) ObjectMetaGet(path string) *skv.ReplyObjectMeta {

	r := db._raw_get(skv.ObjectMetaIndex(path))

	meta := skv.ObjectMetaParse(r.Bytes())
	meta.Reply = r

	return &meta
}

func (db *DB) ObjectMetaScan(path, cursor, end string, limit uint64) *skv.ReplyObjectMetaList {

	if limit > skv.ScanMaxLimit {
		limit = skv.ScanMaxLimit
	}

	var (
		prefix = skv.ObjectMetaFold(path)
		prelen = len(prefix)
		cstart = append(prefix, skv.ObjectStringHex(cursor)...)
		cend   = append(prefix, skv.ObjectStringHex(end)...)
		rpl    = &skv.ReplyObjectMetaList{
			Reply: skv.NewReply(""),
			Items: []skv.ReplyObjectMeta{},
		}
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

		rpl.Items = append(rpl.Items, skv.ObjectMetaParse(iter.Value()))

		limit--
	}

	iter.Release()

	if iter.Error() != nil {
		rpl.Status = iter.Error().Error()
	}

	return rpl
}

func (db *DB) _obj_meta_sync(path string, otype uint8, pnum int32, psize, size int64, ttl uint32) string {

	cpath, fold, field, _, _ := skv.ObjectPathSplit(path)

	var meta, pmeta skv.ReplyObjectMeta

	if psize > 0 && size > 0 {

		if r := db._raw_get(skv.ObjectMetaIndex(cpath)); r.Status == skv.ReplyOK {
			meta = skv.ObjectMetaParse(r.Bytes())
		}
	}

	//
	if size > 0 {
		meta.Len = 0
		meta.Size = uint64(size)
		meta.Version = 0

		//
		if meta.Created < 1 {
			meta.Created = skv.MetaTimeNow()
		}
		meta.Updated = skv.MetaTimeNow()

		meta.Name = field
	}

	_obj_meta_locker.Lock()
	defer _obj_meta_locker.Unlock()

	if r := db._raw_get(skv.ObjectMetaIndex(fold)); r.Status == skv.ReplyOK {

		pmeta = skv.ObjectMetaParse(r.Bytes())

		if pmeta.Len == 0 {
			return skv.ReplyInvalidArgument
		}
	}

	//
	pnum += int32(pmeta.Len)
	if pnum < 0 {
		pnum = 0
	}
	pmeta.Len = uint32(pnum)

	psize += int64(pmeta.Size)
	if psize < 0 {
		psize = 0
	}
	pmeta.Size = uint64(psize)

	//
	if pmeta.Created < 1 {
		pmeta.Created = skv.MetaTimeNow()
	}
	pmeta.Updated = skv.MetaTimeNow()

	if i := strings.LastIndex(fold, "/"); i > 0 {
		pmeta.Name = fold[:i]
	} else {
		pmeta.Name = fold
	}

	//
	batch := new(leveldb.Batch)

	if size > 0 {

		if ok := db._raw_set_ttl(skv.NsObjectEntry, []byte(path), ttl); !ok {
			return "ServerError TTL Set"
		}

		batch.Put(skv.ObjectMetaIndex(cpath), meta.Export())

	} else {
		batch.Delete(skv.ObjectMetaIndex(cpath))
	}

	if pmeta.Len < 1 {
		batch.Delete(skv.ObjectMetaIndex(fold))
	} else {
		batch.Put(skv.ObjectMetaIndex(fold), pmeta.Export())
	}

	if err := db.ldb.Write(batch, nil); err != nil {
		return err.Error()
	}

	return skv.ReplyOK
}
