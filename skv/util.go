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
	"crypto/sha1"
	"io"
)

func stringToHashBytes(str string, num int) []byte {

	if num < 1 {
		num = 1
	} else if num > 20 {
		num = 20
	}

	h := sha1.New()
	io.WriteString(h, str)

	return h.Sum(nil)[:num]
}

func keyLenFilter(key []byte) []byte {

	if len(key) < keyLenMax {
		return key
	}

	return key[:keyLenMax]
}
