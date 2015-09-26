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
	"strings"
)

const (
	QueryFilterValues  uint8 = 0
	QueryFilterRange   uint8 = 1
	QueryOffsetMax           = 100000
	QueryLimitMax            = 10000
	_iquery_filter_max       = 4
)

const (
	QuerySortAttrAsc  = "asc"
	QuerySortAttrDesc = "desc"
)

type QuerySet struct {
	Filters   []QueryFilter
	Limit     int
	Offset    int
	SortField string
	SortMode  string
}

type QueryFilter struct {
	Type    uint8
	Field   string
	Values  []string
	Exclude bool
}

func NewQuery() *QuerySet {

	return &QuerySet{
		Limit:   1,
		Filters: []QueryFilter{},
	}
}

func (q *QuerySet) Filter(field string, values []string, exclude bool) *QuerySet {

	if len(q.Filters) < _iquery_filter_max {

		q.Filters = append(q.Filters, QueryFilter{
			Type:    QueryFilterValues,
			Field:   field,
			Values:  values,
			Exclude: exclude,
		})
	}

	return q
}

func (q *QuerySet) Limits(offset, limit int) *QuerySet {

	if offset > QueryOffsetMax {
		q.Offset = QueryOffsetMax
	} else if offset < 0 {
		q.Offset = 0
	} else {
		q.Offset = offset
	}

	if limit > QueryLimitMax {
		q.Limit = QueryLimitMax
	} else if limit < 1 {
		q.Limit = 1
	} else {
		q.Limit = limit
	}

	return q
}

func (q *QuerySet) Sort(mode, field string) *QuerySet {

	q.SortMode = strings.ToLower(mode)
	if q.SortMode != QuerySortAttrDesc {
		q.SortMode = QuerySortAttrAsc
	}

	q.SortField = strings.ToLower(field)

	return q
}
