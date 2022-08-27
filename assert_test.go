// Copyright 2022 Bogdan Constantinescu.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://github.com/actforgood/bigcsvreader/blob/main/LICENSE.

package bigcsvreader_test

import (
	"reflect"
	"testing"
)

// Note: this file contains some assertion utilities.

// assertEqual checks if 2 values are equal.
// Returns successful assertion status.
func assertEqual(t *testing.T, expected interface{}, actual interface{}) bool {
	t.Helper()
	if !reflect.DeepEqual(expected, actual) {
		t.Errorf(
			"\n\t"+`expected "%+v" (%T),`+
				"\n\t"+`but got  "%+v" (%T)`+"\n",
			expected, expected,
			actual, actual,
		)

		return false
	}

	return true
}

// assertNotNil checks if value passed is not nil.
// Returns successful assertion status.
func assertNotNil(t *testing.T, actual interface{}) bool {
	t.Helper()
	if isNil(actual) {
		t.Error("should not be nil")

		return false
	}

	return true
}

// assertNil checks if value passed is nil.
// Returns successful assertion status.
func assertNil(t *testing.T, actual interface{}) bool {
	t.Helper()
	if !isNil(actual) {
		t.Errorf("expected nil, but got %+v", actual)

		return false
	}

	return true
}

// assertTrue checks if value passed is true.
// Returns successful assertion status.
func assertTrue(t *testing.T, actual bool) bool {
	t.Helper()
	if !actual {
		t.Error("should be true")

		return false
	}

	return true
}

// isNil checks an interface if it is nil.
func isNil(object interface{}) bool {
	if object == nil {
		return true
	}

	value := reflect.ValueOf(object)

	kind := value.Kind()
	switch kind {
	case reflect.Ptr:
		return value.IsNil()
	case reflect.Slice:
		return value.IsNil()
	case reflect.Map:
		return value.IsNil()
	case reflect.Interface:
		return value.IsNil()
	case reflect.Func:
		return value.IsNil()
	case reflect.Chan:
		return value.IsNil()
	}

	return false
}
