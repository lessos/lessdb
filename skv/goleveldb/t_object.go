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
	_obj_meta_locker      sync.Mutex
	_obj_grpstatus_locker sync.Mutex
	_obj_event_handler    skv.ObjectEventHandler
	_obj_options_def      = &skv.ObjectWriteOptions{}
)

func (db *DB) ObjectEventRegister(ev skv.ObjectEventHandler) {

	_obj_meta_locker.Lock()
	defer _obj_meta_locker.Unlock()

	_obj_event_handler = ev
}

func (db *DB) ObjectGet(path string) *skv.Reply {
	return db._raw_get(skv.NewObjectPathParse(path).EntryIndex())
}

func (db *DB) ObjectPut(path string, value interface{}, opts *skv.ObjectWriteOptions) *skv.Reply {

	var (
		opath  = skv.NewObjectPathParse(path)
		bkey   = opath.EntryIndex()
		mkey   = opath.MetaIndex()
		bvalue []byte
	)

	if opts == nil {
		opts = _obj_options_def
	}

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
		return skv.NewReply(skv.ReplyBadArgument)
	}

	meta := db._raw_get(mkey).ObjectMeta()

	db._obj_meta_sync(skv.ObjectTypeGeneral, &meta, opath, int64(len(bvalue)), opts)

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

		db._obj_meta_sync(ms.Type, &ms, opath, -1, _obj_options_def)

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

type _object_journal struct {
	path    []byte
	version uint64
}

func (db *DB) ObjectJournalScan(bucket string, pnum uint32, start, end uint64, limit uint32) *skv.Reply {

	var (
		prefix = skv.BytesConcat([]byte{skv.NsObjectJournal}, skv.NewObjectPathParse(bucket+"/0").BucketBytes(), skv.Uint32ToBytes(pnum))
		cstart = append(prefix, skv.Uint64ToBytes(start)...)
		cend   = prefix
		rpl    = skv.NewReply("")
	)

	if end <= start {
		cend = append(prefix, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}...)
	} else {
		cend = append(prefix, skv.Uint64ToBytes(end)...)
	}

	if limit > uint32(skv.ScanLimitMax) {
		limit = uint32(skv.ScanLimitMax)
	}

	keys := []_object_journal{}

	iter := db.ldb.NewIterator(&util.Range{Start: cstart, Limit: cend}, nil)

	for iter.Next() {

		if limit < 1 {
			break
		}

		if len(iter.Value()) < 1 {
			continue
		}

		keys = append(keys, _object_journal{
			path:    skv.BytesClone(iter.Value()),
			version: skv.BytesToUint64(iter.Key()[len(iter.Key())-8:]),
		})

		limit--
	}

	iter.Release()

	if iter.Error() != nil {
		rpl.Status = iter.Error().Error()
		return rpl
	}

	//
	for _, key := range keys {

		r := db._raw_get(skv.NewObjectPathParse(string(key.path)).EntryIndex())

		if r.Status == skv.ReplyNotFound {

			nil_meta := &skv.ObjectMeta{}

			rpl.Data = append(rpl.Data, key.path)
			rpl.Data = append(rpl.Data, nil_meta.Export())

		} else if r.Status == skv.ReplyOK {

			if key.version == r.ObjectMeta().Version {
				rpl.Data = append(rpl.Data, key.path)
				rpl.Data = append(rpl.Data, r.Bytes())
			}
		}
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

func (db *DB) ObjectJournalVersionIncr(path string, group_number uint32, step int64) *skv.Reply {
	return db._raw_incrby(skv.NewObjectPathParse(path).NsJournalVersionIndex(group_number), step)
}

func (db *DB) ObjectGroupStatus(bucket string, group_number uint32) *skv.Reply {
	return db._raw_get(skv.NewObjectPathParse(bucket + "/0").NsGroupStatusIndex(group_number))
}

func (db *DB) _obj_group_status_sync(bucket_bytes []byte, group_number uint32, evtype uint8, size int64) {

	if evtype == skv.ObjectEventNone {
		return
	}

	if evtype == skv.ObjectEventUpdated && size == 0 {
		return
	}

	_obj_grpstatus_locker.Lock()
	defer _obj_grpstatus_locker.Unlock()

	var (
		key = skv.BytesConcat([]byte{skv.NsObjectGroupStatus}, bucket_bytes, skv.Uint32ToBytes(group_number))
		st  skv.ObjectGroupStatus
	)

	// TOPO
	if rs := db._raw_get(key); rs.Status == skv.ReplyOK {
		rs.JsonDecode(&st)
	} else if rs.Status != skv.ReplyNotFound {
		return
	}

	resize := int64(st.Size) + size

	if resize > 0 {
		st.Size = uint64(resize)
	} else {
		st.Size = 0
	}

	if evtype == skv.ObjectEventCreated {
		st.Num++
	} else if evtype == skv.ObjectEventDeleted && st.Num > 0 {
		st.Num--
	}

	db._raw_put_json(key, st, 0)
}

func (db *DB) _obj_meta_sync(otype byte, meta *skv.ObjectMeta, opath *skv.ObjectPath, size int64, opts *skv.ObjectWriteOptions) string {

	//
	if meta.Type > 0 {
		if meta.Type != otype || meta.Name != opath.FieldName {
			return skv.ReplyBadArgument
		}
	}

	prev_version := meta.Version

	//
	if size >= 0 {

		meta.Version = opts.Version

		//
		if meta.Created < 1 {
			meta.Created = skv.MetaTimeNow()
		}

		meta.Updated = skv.MetaTimeNow()

		meta.Num = 0

		meta.Name = opath.FieldName
	}

	_obj_meta_locker.Lock()
	defer _obj_meta_locker.Unlock()

	var (
		fold_size     int64 = 0
		fold_path           = opath.Parent()
		fold_meta           = db._raw_get(fold_path.MetaIndex()).ObjectMeta()
		gstatus_event       = skv.ObjectEventNone
		gstatus_size  int64 = 0
	)

	if fold_meta.Type > 0 && fold_meta.Type != skv.ObjectTypeFold {
		return skv.ReplyBadArgument
	}

	if fold_meta.Type > 0 && fold_meta.Name != fold_path.FieldName {
		return skv.ReplyBadArgument
	}

	// fmt.Printf("opts.JournalEnable PUT %s/%s, EN:%v, TTL:%d, VER:%d\n", opath.FoldName, opath.FieldName, opts.JournalEnable, opts.Ttl, meta.Version)
	if opts.JournalEnable && meta.Version == 0 {
		meta.Version = db._raw_incrby(opath.NsJournalVersionIndex(opts.GroupNumber), 1).Uint64()
		// fmt.Println("opts.JournalEnable Version NEW", meta.Version)
	}

	if size == -1 {
		gstatus_event = skv.ObjectEventDeleted
	} else if fold_meta.Type > 0 {
		gstatus_event = skv.ObjectEventUpdated
	} else {
		gstatus_event = skv.ObjectEventCreated
	}

	//
	if (size >= 0 && fold_meta.Type < 1) || size < 0 {

		pfp := fold_path.Parent()

		for {

			if pfp.FoldName == "" && pfp.FieldName == "" {
				break
			}

			//
			pfp_meta := db._raw_get(pfp.MetaIndex()).ObjectMeta()
			if pfp_meta.Type > 0 && pfp_meta.Type != skv.ObjectTypeFold {
				return skv.ReplyBadArgument
			}

			pfp_meta.Version = meta.Version

			if pfp_meta.Type < 1 {
				pfp_meta.Name = pfp.FieldName
			}

			if pfp_meta.Created < 1 {
				pfp_meta.Created = skv.MetaTimeNow()
			}
			pfp_meta.Updated = skv.MetaTimeNow()

			//
			found := false
			if pfp_meta.Type > 0 {
				found = true
			}

			if size >= 0 {

				pfp_meta.Num++

				pfp_meta.Type = skv.ObjectTypeFold

				// fmt.Println(pfp.FoldName, pfp.FieldName)

				db._raw_put(pfp.EntryIndex(), pfp_meta.Export(), 0)
				db._raw_put(pfp.MetaIndex(), pfp_meta.Export(), 0)

				if found {
					break
				}

			} else if found {

				if pfp_meta.Num > 1 {

					pfp_meta.Num--

					db._raw_put(pfp.EntryIndex(), pfp_meta.Export(), 0)
					db._raw_put(pfp.MetaIndex(), pfp_meta.Export(), 0)

					break
				}

				db._raw_del(pfp.EntryIndex(), pfp.MetaIndex())

			} else {
				break
			}

			pfp = pfp.Parent()
		}
	}

	//
	fold_meta.Type = skv.ObjectTypeFold
	fold_meta.Name = fold_path.FieldName
	fold_meta.Version = opts.Version

	//
	if size >= 0 && meta.Type < 1 {
		fold_meta.Num++
	} else if size < 0 && fold_meta.Num > 0 {
		fold_meta.Num--
	}
	meta.Type = otype

	if size >= 0 {
		gstatus_size = size - int64(meta.Size)

		fold_size = int64(fold_meta.Size) + (size - int64(meta.Size))
		meta.Size = uint64(size)

	} else {
		gstatus_size = 0 - int64(meta.Size)

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

		if ok := db._raw_ssttl_put(skv.NsObjectEntry, []byte(opath.EntryPath()), opts.Ttl); !ok {
			return "ServerError TTL Set"
		}

		meta.Ttl = opts.Ttl
		batch.Put(opath.MetaIndex(), meta.Export())

		if opts.JournalEnable {

			if prev_version > 0 {
				batch.Delete(opath.NsJournalEntryIndex(opts.GroupNumber, prev_version))
			}

			// batch.Put(opath.NsJournalEntryIndex(opts.GroupNumber, meta.Version), append(opath.Fold, opath.Field...))
			batch.Put(opath.NsJournalEntryIndex(opts.GroupNumber, meta.Version), []byte(opath.EntryPath()))
		}

	} else {
		batch.Delete(opath.MetaIndex())
	}

	if opts.GroupStatusEnable {
		db._obj_group_status_sync(opath.BucketBytes(), opts.GroupNumber, gstatus_event, gstatus_size)
	}

	if fold_meta.Num < 1 {
		batch.Delete(fold_path.MetaIndex())
		batch.Delete(fold_path.EntryIndex())

		// fmt.Println("\t#### fs dir del", fold_path.EntryPath(), fold_meta.Num)
	} else {

		fold_meta.Version = meta.Version

		batch.Put(fold_path.MetaIndex(), fold_meta.Export())
		batch.Put(fold_path.EntryIndex(), fold_meta.Export())

		// fmt.Println("\t#### fs dir add", fold_path.EntryPath(), fold_meta.Num, opath.EntryPath(), meta.Version, skv.MetaTimeNow())
	}

	//
	if err := db.ldb.Write(batch, nil); err != nil {
		return err.Error()
	}

	return skv.ReplyOK
}
