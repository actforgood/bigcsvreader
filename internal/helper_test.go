// Copyright 2022 Bogdan Constantinescu.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://github.com/actforgood/bigcsvreader/blob/main/LICENSE.

package internal_test

import (
	"reflect"
	"testing"

	"github.com/actforgood/bigcsvreader/internal"
)

func TestComputeGoroutineOffsets(t *testing.T) {
	t.Parallel()

	// arrange
	tests := [...]struct {
		name                          string
		inputTotalBytes               int
		inputMaxGoroutines            int
		inputMinBytesReadByAGoroutine int
		expectedResult                [][2]int
	}{
		{
			name:                          "even distribution, min bytes 0 is counted as 1",
			inputTotalBytes:               18,
			inputMaxGoroutines:            3,
			inputMinBytesReadByAGoroutine: 0, // this is adjusted to 1
			expectedResult:                [][2]int{{0, 5}, {6, 11}, {12, 17}},
		},
		{
			name:                          "even distribution, min bytes 1",
			inputTotalBytes:               18,
			inputMaxGoroutines:            3,
			inputMinBytesReadByAGoroutine: 1,
			expectedResult:                [][2]int{{0, 5}, {6, 11}, {12, 17}},
		},
		{
			name:                          "even distribution, min bytes 3",
			inputTotalBytes:               18,
			inputMaxGoroutines:            3,
			inputMinBytesReadByAGoroutine: 3,
			expectedResult:                [][2]int{{0, 5}, {6, 11}, {12, 17}},
		},
		{
			name:                          "even distribution, min bytes is maximum to reach max goroutines",
			inputTotalBytes:               18,
			inputMaxGoroutines:            3,
			inputMinBytesReadByAGoroutine: 6, // 6 x 3 = 18
			expectedResult:                [][2]int{{0, 5}, {6, 11}, {12, 17}},
		},
		{
			name:                          "max goroutines is not reached, even distribution 1",
			inputTotalBytes:               18,
			inputMaxGoroutines:            3,
			inputMinBytesReadByAGoroutine: 6 + 1,
			expectedResult:                [][2]int{{0, 8}, {9, 17}},
		},
		{
			name:                          "max goroutines is not reached, even distribution 2",
			inputTotalBytes:               18,
			inputMaxGoroutines:            3,
			inputMinBytesReadByAGoroutine: 6 + 3,
			expectedResult:                [][2]int{{0, 8}, {9, 17}},
		},
		{
			name:                          "max goroutines is not reached, with reminder",
			inputTotalBytes:               19,
			inputMaxGoroutines:            3,
			inputMinBytesReadByAGoroutine: 9,
			expectedResult:                [][2]int{{0, 8}, {9, 17 + 1}},
		},
		{
			name:                          "max goroutines is not reached, total bytes is < min bytes",
			inputTotalBytes:               18,
			inputMaxGoroutines:            3,
			inputMinBytesReadByAGoroutine: 19,
			expectedResult:                [][2]int{{0, 17}},
		},
		{
			name:                          "max goroutines is not reached, total bytes is = min bytes",
			inputTotalBytes:               18,
			inputMaxGoroutines:            3,
			inputMinBytesReadByAGoroutine: 18,
			expectedResult:                [][2]int{{0, 17}},
		},
		{
			name:                          "max goroutines is not reached, total bytes is a little bigger than min bytes",
			inputTotalBytes:               18,
			inputMaxGoroutines:            3,
			inputMinBytesReadByAGoroutine: 17,
			expectedResult:                [][2]int{{0, 17}},
		},
		{
			name:                          "reminder gets to last goroutine",
			inputTotalBytes:               20,
			inputMaxGoroutines:            3,
			inputMinBytesReadByAGoroutine: 1,
			expectedResult:                [][2]int{{0, 5}, {6, 11}, {12, 17 + 2}},
		},
		{
			name:                          "0 total bytes returns nil result",
			inputTotalBytes:               0,
			inputMaxGoroutines:            3,
			inputMinBytesReadByAGoroutine: 10,
			expectedResult:                nil,
		},
		{
			name:                          "0 max goroutines is counted as 1 goroutine",
			inputTotalBytes:               10,
			inputMaxGoroutines:            0,
			inputMinBytesReadByAGoroutine: 10,
			expectedResult:                [][2]int{{0, 9}},
		},
	}

	for _, testData := range tests {
		test := testData // capture range variable
		t.Run(test.name, func(t *testing.T) {
			// act
			result := internal.ComputeGoroutineOffsets(
				test.inputTotalBytes,
				test.inputMaxGoroutines,
				test.inputMinBytesReadByAGoroutine,
			)

			// assert
			if !reflect.DeepEqual(result, test.expectedResult) {
				t.Errorf("expected %v, but got %v | %s", test.expectedResult, result, test.name)
			}
		})
	}
}

func BenchmarkComputeGoroutineOffsets_1(b *testing.B) {
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		_ = internal.ComputeGoroutineOffsets(1024, 32, 1)
	}
}

func BenchmarkComputeGoroutineOffsets_2(b *testing.B) {
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		_ = internal.ComputeGoroutineOffsets(1024, 32, 1025)
	}
}

func BenchmarkComputeGoroutineOffsets_3(b *testing.B) {
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		_ = internal.ComputeGoroutineOffsets(1024, 32, 1023)
	}
}
