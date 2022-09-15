// Copyright The ActForGood Authors.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://github.com/actforgood/bigcsvreader/blob/main/LICENSE.

// Package internal contains internal logic.
package internal

// Logger logs information while processing a CSV file.
type Logger interface {
	// Debug logs debug information.
	Debug(keyValues ...interface{})
	// Error logs any error occurred.
	Error(keyValues ...interface{})
}

// NopLogger does not log anything.
type NopLogger struct{}

// Debug does nothing.
func (NopLogger) Debug(...interface{}) {}

// Error does nothing.
func (NopLogger) Error(...interface{}) {}
