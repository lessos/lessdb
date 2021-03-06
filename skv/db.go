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

const (
	//
	KiB = 1024
	MiB = KiB * 1024
	GiB = MiB * 1024

	//
	keyLenMax    int    = 200
	ScanLimitMax uint32 = 100000

	// Namespace
	NsRawTtlEntry          = 0x08
	NsRawTtlQueue          = 0x09
	NsKvEntry              = 0x10
	NsHashEntry            = 0x20
	NsHashLength           = 0x21
	NsObjectEntry          = 0x30
	NsObjectMeta           = 0x31
	NsObjectVersionCounter = 0x32
	NsObjectLogEntry       = 0x33
	NsObjectLogCounter     = 0x34
	NsObjectGroupStatus    = 0x35
	nsObjectDocSchema      = 0x38
	nsObjectDocIndex       = 0x39
	nsObjectDocIncrement   = 0x3a
	nsSsEntry              = 0x40
	nsSsScore              = 0x41
	nsSsLength             = 0x42
)

type DB interface {
	KvInterface        // Key Value
	HashInterface      // Hashed Sets
	SsInterface        // Sorted Sets
	ObjectInterface    // Objects
	ObjectDocInterface // Indexed Documents
	Close()
}
