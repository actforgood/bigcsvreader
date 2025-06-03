// Copyright The ActForGood Authors.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://github.com/actforgood/bigcsvreader/blob/main/LICENSE.

package internal

// ComputeGoroutineOffsets computes how many goroutines will handle totalBytes, and their [start, end] offset intervals.
// First arguments represents the total bytes to be handled.
// Second argument represents the maximum goroutines that will handle total bytes.
// Third argument represents the minimum bytes a goroutine should handle.
// It returns a slice (up to maxGoroutines in length) of [start, end] intervals each goroutine should handle.
func ComputeGoroutineOffsets(totalBytes, maxGoroutines, minBytesReadByAGoroutine int) [][2]int {
	// make some checks
	if totalBytes <= 0 {
		return nil
	}
	if minBytesReadByAGoroutine <= 0 {
		minBytesReadByAGoroutine = 1
	}
	if maxGoroutines <= 0 {
		maxGoroutines = 1
	}

	// skip rest of computations and return immediately if total bytes < min bytes,
	// it means we only have 1 goroutine reading total bytes.
	if totalBytes <= minBytesReadByAGoroutine {
		return [][2]int{{0, totalBytes - 1}}
	}

	totalGoroutines := totalBytes / minBytesReadByAGoroutine
	if totalGoroutines == 1 {
		return [][2]int{{0, totalBytes - 1}}
	}
	if totalGoroutines > maxGoroutines {
		totalGoroutines = maxGoroutines
	}
	bytesPerGoroutine := totalBytes / totalGoroutines
	distribution := make([][2]int, totalGoroutines)
	start, end := 0, bytesPerGoroutine-1
	for goroutineNo := range totalGoroutines - 1 {
		distribution[goroutineNo] = [2]int{start, end}
		start = end + 1
		end += bytesPerGoroutine
	}
	distribution[totalGoroutines-1] = [2]int{start, totalBytes - 1}

	return distribution
}
