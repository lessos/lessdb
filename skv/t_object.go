// Copyright 2015-2016 lessdb Author, All rights reserved.
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
	"strings"

	"github.com/lessos/lessdb/dbtypes"
	"github.com/lessos/lessdb/dbutil"
)

const (
	ObjectEventNone    uint8 = 0
	ObjectEventCreated uint8 = 1
	ObjectEventUpdated uint8 = 2
	ObjectEventDeleted uint8 = 3

	ObjectFoldLength  = 12
	ObjectFieldLength = 8
)

type ObjectInterface interface {
	//
	ObjectEventRegister(ev ObjectEventHandler)
	//
	ObjectGet(path string) *Reply
	ObjectPut(path string, value interface{}, opts *ObjectWriteOptions) *Reply
	ObjectDel(path string) *Reply
	ObjectScan(fold, cursor, end string, limit uint32) *Reply
	//
	ObjectMetaGet(path string) *Reply
	ObjectMetaScan(fold, cursor, end string, limit uint32) *Reply
	ObjectMetaVersionIncr(path string, group_number uint32, step int64) *Reply
	ObjectGroupStatus(bucket string, group_number uint32) *Reply
	ObjectLogScan(bucket string, group_number uint32, start, end uint64, limit uint32) *ObjectLogReply
}

type ObjectDocInterface interface {
	ObjectDocSchemaSync(fold string, schema ObjectDocSchema) *Reply
	ObjectDocGet(fold, key string) *Reply
	ObjectDocPut(fold, key string, value interface{}, opts *ObjectWriteOptions) *Reply
	ObjectDocDel(fold, key string) *Reply
	ObjectDocQuery(fold string, qry *ObjectDocQuerySet) *Reply
}

type ObjectEventHandler func(opath *ObjectPath, evtype uint8, version uint64)

type ObjectWriteOptions struct {
	Updated           uint64
	Expired           uint64
	Ttl               int64
	Version           uint64
	LogEnable         bool
	GroupNumber       uint32
	GroupStatusEnable bool
}

type ObjectLogReply struct {
	Reply
	Offset uint64
	Offcut uint64
}

//
type ObjectPath struct {
	Fold      []byte
	FoldName  string
	Field     []byte
	FieldName string
}

type ObjectGroupStatus struct {
	Size       uint64 `json:"size"`
	Num        uint64 `json:"num"`
	ObjVersion uint64 `json:"obj_version"`
	LogVersion uint64 `json:"log_version"`
}

func (op *ObjectPath) EntryIndex() []byte {
	return append(RawNsKeyConcat(NsObjectEntry, op.Fold), op.Field...)
}

func (op *ObjectPath) MetaIndex() []byte {
	return append(RawNsKeyConcat(NsObjectMeta, op.Fold), op.Field...)
}

func (op *ObjectPath) EntryPath() string {
	return dbutil.ObjectPathClean(op.FoldName + "/" + op.FieldName)
}

func (op *ObjectPath) Parent() *ObjectPath {
	return NewObjectPathParse(op.FoldName)
}

func (op *ObjectPath) BucketName() string {

	r := op.FoldName
	if i := strings.Index(op.FoldName, "/"); i > 0 {
		r = op.FoldName[:i]
	}

	return r
}

func (op *ObjectPath) BucketBytes() []byte {
	return stringToHashBytes(op.BucketName(), 4) // 2^32
}

func (op *ObjectPath) BucketID() uint32 {
	return dbutil.BytesToUint32(op.BucketBytes())
}

func (op *ObjectPath) NsVersionCounterIndex(group_number uint32) []byte {
	return dbutil.BytesConcat([]byte{NsObjectVersionCounter}, op.BucketBytes(), dbutil.Uint32ToBytes(group_number))
}

func (op *ObjectPath) NsLogCounterIndex(group_number uint32) []byte {
	return dbutil.BytesConcat([]byte{NsObjectLogCounter}, op.BucketBytes(), dbutil.Uint32ToBytes(group_number))
}

func (op *ObjectPath) NsLogEntryIndex(group_number uint32, num uint64) []byte {
	return dbutil.BytesConcat([]byte{NsObjectLogEntry}, op.BucketBytes(), dbutil.Uint32ToBytes(group_number), dbutil.Uint64ToBytes(num))
}

func (op *ObjectPath) NsGroupStatusIndex(group_number uint32) []byte {
	return dbutil.BytesConcat([]byte{NsObjectGroupStatus}, op.BucketBytes(), dbutil.Uint32ToBytes(group_number))
}

//
func NewObjectPathKey(fold, key string) *ObjectPath {

	op := &ObjectPath{
		FoldName: dbutil.ObjectPathClean(fold),
	}

	op.Fold = stringToHashBytes(op.FoldName, ObjectFoldLength)

	klen := len(key)
	if klen > 32 {
		klen = 32
	}

	if v := dbutil.HexStringToBytes(key[:klen]); len(v) > 0 {
		op.Field = v
		op.FieldName = key[:klen]
	}

	return op
}

func NewObjectPathParse(path string) *ObjectPath {

	op := &ObjectPath{}

	is_fold := false
	if len(path) > 0 && path[len(path)-1] == '/' {
		is_fold = true
	}

	path = dbutil.ObjectPathClean(path)

	if is_fold {
		op.FoldName, op.FieldName = path, ""
	} else {
		if i := strings.LastIndex(path, "/"); i > 0 {
			op.FoldName, op.FieldName = path[:i], path[i+1:]
		} else {
			op.FoldName, op.FieldName = "", path
		}
	}

	op.Fold = stringToHashBytes(op.FoldName, ObjectFoldLength)
	op.Field = stringToHashBytes(op.FieldName, ObjectFieldLength)

	return op
}

func ObjectNsEntryFoldKey(path string) []byte {
	return RawNsKeyConcat(NsObjectEntry, stringToHashBytes(dbutil.ObjectPathClean(path), ObjectFoldLength))
}

func ObjectNsMetaFoldKey(path string) []byte {
	return RawNsKeyConcat(NsObjectMeta, stringToHashBytes(dbutil.ObjectPathClean(path), ObjectFoldLength))
}

type Object struct {
	Data   dbtypes.Bytex
	Status string
	Meta   ObjectMeta
	Key    []byte
}
