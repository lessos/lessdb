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
	"crypto/rand"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"io"
	"path/filepath"
	"strings"
	// "fmt"
)

const (
	ObjectTypeFold     = 0x01
	ObjectTypeGeneral  = 0x0a
	ObjectTypeDocument = 0x0b

	ObjectEventCreated uint8 = 1
	ObjectEventUpdated uint8 = 2
	ObjectEventDeleted uint8 = 3

	obj_fold_len  = 12
	obj_field_len = 8
)

type ObjectEventHandler func(opath *ObjectPath, evtype uint8, version uint64)

type ObjectInterface interface {
	//
	ObjectEventRegister(ev ObjectEventHandler)
	//
	ObjectGet(path string) *Reply
	ObjectSet(path string, value interface{}, ttl uint32) *Reply
	ObjectDel(path string) *Reply
	ObjectScan(fold, cursor, end string, limit uint32) *Reply
	//
	ObjectMetaGet(path string) *Reply
	ObjectMetaScan(fold, cursor, end string, limit uint64) *Reply
	//
	ObjectDocSchemaSync(fold string, schema ObjectDocSchema) *Reply
	ObjectDocGet(fold, key string) *Reply
	ObjectDocSet(fold, key string, value interface{}, ttl uint32) *Reply
	ObjectDocDel(fold, key string) *Reply
	ObjectDocQuery(fold string, qry *ObjectDocQuerySet) *Reply
}

type ObjectPath struct {
	Fold      []byte
	FoldName  string
	Field     []byte
	FieldName string
}

func (op *ObjectPath) EntryIndex() []byte {
	return append(_obj_entry_key_prefix(op.Fold), op.Field...)
}

func (op *ObjectPath) MetaIndex() []byte {
	return append(_obj_entry_meta_prefix(op.Fold), op.Field...)
}

func (op *ObjectPath) EntryPath() string {
	// return ObjectPathClean(op.FoldName + "/" + hex.EncodeToString(op.Field))
	return ObjectPathClean(op.FoldName + "/" + op.FieldName)
}

func (op *ObjectPath) Parent() *ObjectPath {
	return NewObjectPathParse(op.FoldName)
}

func NewObjectPathKey(fold, key string) *ObjectPath {

	op := &ObjectPath{
		FoldName: ObjectPathClean(fold),
	}

	op.Fold = _obj_str_hash(op.FoldName, obj_fold_len)

	klen := len(key)
	if klen > 32 {
		klen = 32
	}

	if v, err := hex.DecodeString(key[:klen]); err == nil {
		op.Field = v
		op.FieldName = key[:klen]
	}

	return op
}

func NewObjectPathParse(path string) *ObjectPath {

	op := &ObjectPath{}

	path = ObjectPathClean(path)

	if i := strings.LastIndex(path, "/"); i > 0 {
		op.FoldName, op.FieldName = path[:i], path[i+1:]
	} else {
		op.FoldName, op.FieldName = "", path
	}

	op.Fold = _obj_str_hash(op.FoldName, obj_fold_len)
	op.Field = _obj_str_hash(op.FieldName, obj_field_len)

	return op
}

type Object struct {
	EntryValue
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

func ObjectPathClean(path string) string {
	return strings.Trim(filepath.Clean(path), "/")
}

func _obj_str_hash(str string, num int) []byte {

	if num < 1 {
		num = 1
	} else if num > 32 {
		num = 32
	}

	h := sha1.New()
	io.WriteString(h, str)

	return h.Sum(nil)[:num]
}

func _obj_entry_key_prefix(key []byte) []byte {

	si := len(key)
	if si > 255 {
		si = 255
	}

	return append([]byte{NsObjectEntry, uint8(si)}, key[:si]...)
}

func _obj_entry_meta_prefix(key []byte) []byte {
	return append([]byte{ns_object_meta, uint8(len(key))}, key...)
}

func ObjectRandomKey(length int) string {

	if length < 1 {
		length = 1
	} else if length > 16 {
		length = 16
	}

	key := make([]byte, length)

	io.ReadFull(rand.Reader, key)

	// return fmt.Sprintf("%x", key)
	return hex.EncodeToString(key)
}

func ObjectStringHex(key string) []byte {

	if v, err := hex.DecodeString(key); err == nil {
		return v
	}

	return []byte{}
}

func ObjectHexString(key []byte) string {
	return hex.EncodeToString(key)
}

func ObjectEntryFold(path string) []byte {
	return _obj_entry_key_prefix(_obj_str_hash(ObjectPathClean(path), obj_fold_len))
}

func ObjectMetaFold(path string) []byte {
	return _obj_entry_meta_prefix(_obj_str_hash(ObjectPathClean(path), obj_fold_len))
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
