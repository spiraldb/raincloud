// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package main

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/util"
)

// Size the selected values, not a fraction of the backing buffers: large
// values can be concentrated in a small slice of a canonical batch.
func decodedSize(a arrow.Array) int64 {
	validity := int64((a.Len() + 7) / 8)
	if ext, ok := a.(array.ExtensionArray); ok {
		return decodedSize(ext.Storage())
	}
	if values, ok := a.(*array.Dictionary); ok {
		size := validity
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				continue
			}
			index := int64(values.GetValueIndex(i))
			part := array.NewSlice(values.Dictionary(), index, index+1)
			size += decodedSize(part)
			part.Release()
		}
		return size
	}
	if dt, ok := a.DataType().(arrow.FixedWidthDataType); ok {
		return validity + (int64(a.Len())*int64(dt.BitWidth())+7)/8
	}
	size := validity
	switch values := a.(type) {
	case interface{ Value(int) string }:
		size += int64(a.Len()+1) * 8
		for i := 0; i < a.Len(); i++ {
			size += int64(len(values.Value(i)))
		}
	case interface{ Value(int) []byte }:
		size += int64(a.Len()+1) * 8
		for i := 0; i < a.Len(); i++ {
			size += int64(len(values.Value(i)))
		}
	case *array.Struct:
		for i := 0; i < values.NumField(); i++ {
			size += decodedSize(values.Field(i))
		}
	case listArray:
		size += int64(a.Len()+1) * 16
		for i := 0; i < a.Len(); i++ {
			lo, hi := values.ValueOffsets(i)
			part := array.NewSlice(values.ListValues(), lo, hi)
			size += decodedSize(part)
			part.Release()
		}
	default:
		// For uncommon representations, overcount backing buffers rather
		// than underestimate memory retained by the row group.
		return util.TotalArraySize(a)
	}
	return size
}
