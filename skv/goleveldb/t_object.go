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
	"fmt"
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
	return db._raw_get(skv.NewObjectPathParse(path).EntryIndex())
}

func (db *DB) ObjectPut(path string, value interface{}, ttl uint32) *skv.Reply {

	var (
		opath  = skv.NewObjectPathParse(path)
		bkey   = opath.EntryIndex()
		mkey   = opath.MetaIndex()
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

	meta := db._raw_get(mkey).ObjectMeta()

	db._obj_meta_sync(skv.ObjectTypeGeneral, &meta, opath, int64(len(bvalue)), ttl)

	return db._raw_put(bkey, append(meta.Export(), bvalue...), 0)
}

func (db *DB) ObjectDel(path string) *skv.Reply {

	var (
		rpl   = skv.NewReply("")
		opath = skv.NewObjectPathParse(path)
	)

	if rs := db._raw_get(opath.MetaIndex()); rs.Status == skv.ReplyOK {

		rpl = db._raw_del(opath.EntryIndex())
		if rpl.Status != skv.ReplyOK {
			return rpl
		}

		ms := rs.ObjectMeta()

		db._obj_meta_sync(ms.Type, &ms, opath, -1, 0)

		if _obj_event_handler != nil {
			_obj_event_handler(opath, skv.ObjectEventDeleted, 0)
		}
	}

	return rpl
}

func (db *DB) ObjectScan(fold, cursor, end string, limit uint32) *skv.Reply {

	var (
		prefix = skv.ObjectNsEntryFoldKey(fold)
		prelen = len(prefix)
		cstart = append(prefix, skv.HexStringToBytes(cursor)...)
		cend   = append(prefix, skv.HexStringToBytes(end)...)
		rpl    = skv.NewReply("")
	)

	for i := len(cend); i < 256; i++ {
		cend = append(cend, 0xff)
	}

	if limit > uint32(skv.ScanLimitMax) {
		limit = uint32(skv.ScanLimitMax)
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
	return db._raw_get(skv.NewObjectPathParse(path).MetaIndex())
}

func (db *DB) ObjectMetaScan(fold, cursor, end string, limit uint32) *skv.Reply {

	var (
		prefix = skv.ObjectNsMetaFoldKey(fold)
		prelen = len(prefix)
		cstart = append(prefix, skv.HexStringToBytes(cursor)...)
		cend   = append(prefix, skv.HexStringToBytes(end)...)
		rpl    = skv.NewReply("")
	)

	for i := len(cend); i < 256; i++ {
		cend = append(cend, 0xff)
	}

	if limit > skv.ScanLimitMax {
		limit = skv.ScanLimitMax
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

func (db *DB) _obj_meta_sync(otype byte, meta *skv.ObjectMeta, opath *skv.ObjectPath, size int64, ttl uint32) string {

	//
	if meta.Type > 0 {
		if meta.Type != otype || meta.Name != opath.FieldName {
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

		meta.Name = opath.FieldName
	}

	_obj_meta_locker.Lock()
	defer _obj_meta_locker.Unlock()

	var (
		fold_size int64
		fold_path = opath.Parent()
		fold_meta = db._raw_get(fold_path.MetaIndex()).ObjectMeta()
	)

	if fold_meta.Type > 0 && fold_meta.Type != skv.ObjectTypeFold {
		return skv.ReplyInvalidArgument
	}

	if fold_meta.Type > 0 && fold_meta.Name != fold_path.FieldName {
		return skv.ReplyInvalidArgument
	}

	//
	fold_meta.Type = skv.ObjectTypeFold
	fold_meta.Name = fold_path.FieldName

	//
	if size >= 0 && meta.Type < 1 {
		fold_meta.Len++
	} else if size < 0 && fold_meta.Len > 0 {
		fold_meta.Len--
	}
	meta.Type = otype

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

		if ok := db._raw_ssttl_put(skv.NsObjectEntry, []byte(opath.EntryPath()), ttl); !ok {
			return "ServerError TTL Set"
		}

		batch.Put(opath.MetaIndex(), meta.Export())

	} else {
		batch.Delete(opath.MetaIndex())
	}

	if fold_meta.Len < 1 {
		batch.Delete(fold_path.MetaIndex())
	} else {
		batch.Put(fold_path.MetaIndex(), fold_meta.Export())
	}

	if err := db.ldb.Write(batch, nil); err != nil {
		return err.Error()
	}

	return skv.ReplyOK
}
