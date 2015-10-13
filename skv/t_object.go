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
	"strings"
)

const (
	ObjectTypeFold     = 0x01
	ObjectTypeGeneral  = 0x0a
	ObjectTypeDocument = 0x0b

	ObjectEventCreated uint8 = 1
	ObjectEventUpdated uint8 = 2
	ObjectEventDeleted uint8 = 3

	_obj_fold_len  = 12
	_obj_field_len = 8
)

type ObjectInterface interface {
	//
	ObjectEventRegister(ev ObjectEventHandler)
	//
	ObjectGet(path string) *Reply
	ObjectPut(path string, value interface{}, opts *ObjectPutOptions) *Reply
	ObjectDel(path string) *Reply
	ObjectScan(fold, cursor, end string, limit uint32) *Reply
	//
	ObjectMetaGet(path string) *Reply
	ObjectMetaScan(fold, cursor, end string, limit uint32) *Reply
}

type ObjectDocInterface interface {
	ObjectDocSchemaSync(fold string, schema ObjectDocSchema) *Reply
	ObjectDocGet(fold, key string) *Reply
	ObjectDocPut(fold, key string, value interface{}, opts *ObjectPutOptions) *Reply
	ObjectDocDel(fold, key string) *Reply
	ObjectDocQuery(fold string, qry *ObjectDocQuerySet) *Reply
}

type ObjectEventHandler func(opath *ObjectPath, evtype uint8, version uint64)

type ObjectPutOptions struct {
	Ttl            uint32
	Version        uint64
	JournalEnable  bool
	PlacementGroup uint32
}

//
type ObjectPath struct {
	Fold      []byte
	FoldName  string
	Field     []byte
	FieldName string
}

func (op *ObjectPath) EntryIndex() []byte {
	return append(RawNsKeyEncode(NsObjectEntry, op.Fold), op.Field...)
}

func (op *ObjectPath) MetaIndex() []byte {
	return append(RawNsKeyEncode(nsObjectMeta, op.Fold), op.Field...)
}

func (op *ObjectPath) EntryPath() string {
	return _filepath_clean(op.FoldName + "/" + op.FieldName)
}

func (op *ObjectPath) Parent() *ObjectPath {
	return NewObjectPathParse(op.FoldName)
}

//
func NewObjectPathKey(fold, key string) *ObjectPath {

	op := &ObjectPath{
		FoldName: _filepath_clean(fold),
	}

	op.Fold = _string_to_hash_bytes(op.FoldName, _obj_fold_len)

	klen := len(key)
	if klen > 32 {
		klen = 32
	}

	if v := HexStringToBytes(key[:klen]); len(v) > 0 {
		op.Field = v
		op.FieldName = key[:klen]
	}

	return op
}

func NewObjectPathParse(path string) *ObjectPath {

	op := &ObjectPath{}

	path = _filepath_clean(path)

	if i := strings.LastIndex(path, "/"); i > 0 {
		op.FoldName, op.FieldName = path[:i], path[i+1:]
	} else {
		op.FoldName, op.FieldName = "", path
	}

	op.Fold = _string_to_hash_bytes(op.FoldName, _obj_fold_len)
	op.Field = _string_to_hash_bytes(op.FieldName, _obj_field_len)

	return op
}

func ObjectNsEntryFoldKey(path string) []byte {
	return RawNsKeyEncode(NsObjectEntry, _string_to_hash_bytes(_filepath_clean(path), _obj_fold_len))
}

func ObjectNsMetaFoldKey(path string) []byte {
	return RawNsKeyEncode(nsObjectMeta, _string_to_hash_bytes(_filepath_clean(path), _obj_fold_len))
}

type Object struct {
	entryValue
	Status string
	Meta   ObjectMeta
	Key    []byte
}

// common
//  - mtype    1 0:1
//  - seek_len 2 1:3
//
//  - version  8 3:11
//  - size     8 11:19
//  - created  8 19:27
//  - updated  8 27:35
//
//  - group    4 35:39
//  - user     4 39:43
//  - mode     1 43:44
//  - ttl      4 44:48
//
// fold
//  - len      4 48:52
// entry
//	- sumcheck 4 48:52
//
// common
//  - name_len 1 52:53
//  - name     n
type ObjectMeta struct {
	seek_len int
	Type     uint8  `json:"type"`
	Version  uint64 `json:"version,omitempty"`
	Size     uint64 `json:"size"`
	Created  uint64 `json:"created"`
	Updated  uint64 `json:"updated"`
	Group    uint32 `json:"group,omitempty"`
	User     uint32 `json:"user,omitempty"`
	Mode     uint8  `json:"mode,omitempty"`
	Ttl      uint32 `json:"ttl,omitempty"`
	Len      uint32 `json:"len,omitempty"`
	SumCheck uint32 `json:"sumcheck,omitempty"`
	Name     string `json:"name"`
}

func (m *ObjectMeta) Export() []byte {

	data := make([]byte, 53)

	//
	data[0] = m.Type

	//
	binary.BigEndian.PutUint64(data[3:11], m.Version)
	binary.BigEndian.PutUint64(data[11:19], m.Size)
	binary.BigEndian.PutUint64(data[19:27], m.Created)
	binary.BigEndian.PutUint64(data[27:35], m.Updated)

	//

	binary.BigEndian.PutUint32(data[35:39], m.Group)
	binary.BigEndian.PutUint32(data[39:43], m.User)
	binary.BigEndian.PutUint32(data[44:48], m.Ttl)

	//
	if m.Type == ObjectTypeFold {
		binary.BigEndian.PutUint32(data[48:52], m.Len)
	} else if m.Type == ObjectTypeGeneral {
		binary.BigEndian.PutUint32(data[48:52], m.SumCheck)
	}

	//
	namelen := len(m.Name)
	if namelen > 200 {
		namelen = 200
	}

	if namelen > 0 {
		data[52] = uint8(namelen)
		data = append(data, []byte(m.Name[:namelen])...)
	}

	binary.BigEndian.PutUint16(data[1:3], uint16(len(data)))

	return data
}

func ObjectMetaParse(data []byte) ObjectMeta {

	m := ObjectMeta{}

	if len(data) > 53 {

		//
		m.Type = data[0]
		m.seek_len = int(binary.BigEndian.Uint16(data[1:3]))

		//
		m.Version = binary.BigEndian.Uint64(data[3:11])
		m.Size = binary.BigEndian.Uint64(data[11:19])
		m.Created = binary.BigEndian.Uint64(data[19:27])
		m.Updated = binary.BigEndian.Uint64(data[27:35])

		//
		m.Group = binary.BigEndian.Uint32(data[35:39])
		m.User = binary.BigEndian.Uint32(data[39:43])
		m.Mode = data[43]
		m.Ttl = binary.BigEndian.Uint32(data[44:48])

		//
		if m.Type == ObjectTypeFold {
			m.Len = binary.BigEndian.Uint32(data[48:52])
		} else if m.Type == ObjectTypeGeneral {
			m.SumCheck = binary.BigEndian.Uint32(data[48:52])
		}

		//
		if len(data) >= m.seek_len {
			// m.NameLen = data[52]
			m.Name = string(data[53:m.seek_len])
		}
	}

	return m
}
