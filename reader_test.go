// Copyright The ActForGood Authors.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://github.com/actforgood/bigcsvreader/blob/main/LICENSE.

package bigcsvreader_test

import (
	"context"
	"encoding/csv"
	"errors"
	"io"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/actforgood/bigcsvreader"
)

func TestCsvReader(t *testing.T) {
	t.Parallel()

	t.Run("csv file with header", testCsvReaderByHeader(true))
	t.Run("csv file without header", testCsvReaderByHeader(false))
	t.Run("empty file", testCsvReaderWithEmptyFile)
	t.Run("not found file", testCsvReaderWithNotFoundFile)
	t.Run("with 10k rows file", testCsvReaderWithDifferentFileSizesAndMaxGoroutines(1e4))
	t.Run("with 100k rows file", testCsvReaderWithDifferentFileSizesAndMaxGoroutines(1e5))
	t.Run("with 500k rows file", testCsvReaderWithDifferentFileSizesAndMaxGoroutines(5e5))
	t.Run("context is canceled", testCsvReaderWithContextCanceled)
	t.Run("invalid row", testCsvReaderWithInvalidRow)
	t.Run("small buffer size", testCsvReaderWithSmallBufferSize)
	t.Run("quotes in unquoted field", testCsvReaderWithLazyQuotes)
}

func testCsvReaderByHeader(withHeader bool) func(t *testing.T) {
	return func(t *testing.T) {
		t.Parallel()

		// arrange
		subject := bigcsvreader.New()
		subject.ColumnsCount = 3
		if withHeader {
			subject.SetFilePath("testdata/file_with_header.csv")
			subject.FileHasHeader = true
			subject.ColumnsDelimiter = ';'
		} else {
			subject.SetFilePath("testdata/file_without_header.csv")
		}

		ctx, cancelCtx := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancelCtx()
		expectedRecords := [][]string{
			{"1", "John", "33"},
			{"2", "Jane", "30"},
			{"3", "Mike", "18"},
			{"4", "Ronaldinho", "23"},
			{"5", "Elisabeth", "45"},
		}

		// act
		rowsChans, errsChan := subject.Read(ctx)
		records, err := gatherRecords(rowsChans, errsChan)

		// assert
		assertNil(t, err)
		assertEqual(t, len(expectedRecords), len(records))
		for _, expectedRecord := range expectedRecords {
			found := false
			for _, record := range records {
				if reflect.DeepEqual(expectedRecord, record) {
					found = true

					break
				}
			}
			if !found {
				t.Errorf("record '%v' was expected to be found, but was not", expectedRecord)
			}
		}
	}
}

func testCsvReaderWithEmptyFile(t *testing.T) {
	t.Parallel()

	// arrange
	subject := bigcsvreader.New()
	subject.SetFilePath("testdata/empty.csv")
	ctx, cancelCtx := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelCtx()
	expectedErr := bigcsvreader.ErrEmptyFile

	// act
	rowsChans, errsChan := subject.Read(ctx)
	records, err := gatherRecords(rowsChans, errsChan)

	// assert
	assertTrue(t, errors.Is(err, expectedErr))
	assertNil(t, records)
}

func testCsvReaderWithNotFoundFile(t *testing.T) {
	t.Parallel()

	// arrange
	subject := bigcsvreader.New()
	subject.SetFilePath("testdata/this_file_does_not_exist.csv")
	ctx, cancelCtx := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelCtx()
	expectedErr := os.ErrNotExist

	// act
	rowsChans, errsChan := subject.Read(ctx)
	records, err := gatherRecords(rowsChans, errsChan)

	// assert
	assertTrue(t, errors.Is(err, expectedErr))
	assertNil(t, records)
}

func testCsvReaderWithDifferentFileSizesAndMaxGoroutines(rowsCount int64) func(t *testing.T) {
	return func(t *testing.T) {
		t.Parallel()

		// arrange
		fName, err := setUpTmpCsvFile(rowsCount)
		if err != nil {
			t.Fatalf("prerequisite failed: could not generate CSV file: %v", err)
		}
		defer tearDownTmpCsvFile(fName)
		subject := bigcsvreader.New()
		subject.SetFilePath(fName)
		subject.ColumnsCount = 5
		ctx, cancelCtx := context.WithCancel(context.Background())
		defer cancelCtx()
		var sumIds int64
		var wg sync.WaitGroup

		for maxGoroutines := 1; maxGoroutines <= 16; maxGoroutines++ {
			subject.MaxGoroutinesNo = maxGoroutines
			sumIds = 0
			expectedSumIds := rowsCount * (rowsCount + 1) / 2

			// act
			rowsChans, errsChan := subject.Read(ctx)

			// assert
			for i := 0; i < len(rowsChans); i++ {
				wg.Add(1)
				go func(rowsChan bigcsvreader.RowsChan, waitGr *sync.WaitGroup) {
					var localSumIds int64
					for record := range rowsChan {
						if !assertEqual(t, 5, len(record)) {
							continue
						}
						id, _ := strconv.ParseInt(record[colID], 10, 64)
						localSumIds += id
						expectedColName := colValueNamePrefix + record[colID]
						assertEqual(t, expectedColName, record[colName])
						assertEqual(t, colValueDescription, record[colDescription])
						assertEqual(t, colValuePrice, record[colPrice])
						assertEqual(t, colValueStock, record[colStock])
					}
					atomic.AddInt64(&sumIds, localSumIds)
					waitGr.Done()
				}(rowsChans[i], &wg)
			}
			for err := range errsChan {
				assertNil(t, err)
			}
			wg.Wait()
			assertEqual(t, expectedSumIds, sumIds)
		}
	}
}

func testCsvReaderWithLazyQuotes(t *testing.T) {
	t.Parallel()

	// arrange
	subject := bigcsvreader.New()
	subject.SetFilePath("testdata/file_with_quote_in_unquoted_field.csv")
	subject.ColumnsCount = 3
	subject.FileHasHeader = false
	subject.LazyQuotes = true

	expectedRecords := [][]string{
		{"1", "John \"The Bomb\" Miguel", "33"},
		{"2", "Jane", "30"},
		{"3", "Mike", "18"},
		{"4", "Ronaldinho", "23"},
		{"5", "Elisabeth", "45"},
	}

	ctx, cancelCtx := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelCtx()

	// act
	rowsChans, errsChan := subject.Read(ctx)
	records, err := gatherRecords(rowsChans, errsChan)

	// assert
	assertNil(t, err)
	assertEqual(t, len(expectedRecords), len(records))
	for _, expectedRecord := range expectedRecords {
		found := false
		for _, record := range records {
			if reflect.DeepEqual(expectedRecord, record) {
				found = true

				break
			}
		}
		if !found {
			t.Errorf("record '%v' was expected to be found, but was not", expectedRecord)
		}
	}
}

func testCsvReaderWithInvalidRow(t *testing.T) {
	t.Parallel()

	// arrange
	subject := bigcsvreader.New()
	subject.SetFilePath("testdata/invalid_row.csv")
	subject.ColumnsCount = 3
	subject.FileHasHeader = true
	var expectedErr *csv.ParseError

	ctx, cancelCtx := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelCtx()

	// act
	rowsChans, errsChan := subject.Read(ctx)
	records, err := gatherRecords(rowsChans, errsChan)

	// assert
	assertTrue(t, errors.As(err, &expectedErr))
	assertNil(t, records)
}

func testCsvReaderWithContextCanceled(t *testing.T) {
	t.Parallel()

	// arrange
	subject := bigcsvreader.New()
	subject.SetFilePath("testdata/file_without_header.csv")
	subject.ColumnsCount = 3
	expectedErr := context.Canceled

	ctx, cancelCtx := context.WithCancel(context.Background())

	// act
	cancelCtx()
	rowsChans, errsChan := subject.Read(ctx)
	records, err := gatherRecords(rowsChans, errsChan)

	// assert
	assertTrue(t, errors.Is(err, expectedErr))
	assertNil(t, records)
}

func testCsvReaderWithSmallBufferSize(t *testing.T) {
	t.Parallel()

	// arrange
	subject := bigcsvreader.New()
	subject.SetFilePath("testdata/file_without_header.csv")
	subject.ColumnsCount = 3
	subject.BufferSize = 16 // min buffer size set by bufio - Ronaldinho line has len 17 and err should arise

	ctx, cancelCtx := context.WithCancel(context.Background())
	defer cancelCtx()

	// act
	rowsChans, errsChan := subject.Read(ctx)
	records, err := gatherRecords(rowsChans, errsChan)

	// assert
	if assertNotNil(t, err) {
		assertTrue(t, strings.Contains(err.Error(), "buffer full"))
	}
	assertNil(t, records)
}

// gatherRecords returns the rows from big csv reader, or an error if something bad happened.
func gatherRecords(rowsChans []bigcsvreader.RowsChan, errsChan bigcsvreader.ErrsChan) ([][]string, error) {
	var (
		mu      sync.Mutex
		wg      sync.WaitGroup
		records = make([][]string, 0)
	)
	for i := 0; i < len(rowsChans); i++ {
		wg.Add(1)
		go func(rowsChan bigcsvreader.RowsChan, mutex *sync.Mutex, waitGr *sync.WaitGroup) {
			for record := range rowsChan {
				mutex.Lock()
				records = append(records, record)
				mu.Unlock()
			}
			waitGr.Done()
		}(rowsChans[i], &mu, &wg)
	}

	for err := range errsChan {
		return nil, err
	}
	wg.Wait()

	return records, nil
}

const (
	colID = iota
	colName
	colDescription
	colPrice
	colStock
)

const (
	colValueNamePrefix  = "Product_"
	colValueDescription = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nunc eleifend felis quis magna auctor, ut lacinia eros efficitur. Maecenas mattis dolor a pharetra gravida. Aenean at eros sed metus posuere feugiat in vitae libero. Morbi a diam volutpat, tempor lacus sed, sagittis velit. Donec eget dignissim mauris, sed aliquam ex. Duis eros dolor, vestibulum ac aliquam eget, viverra in enim. Aenean ut turpis quis purus porta lobortis. Etiam sollicitudin lectus vitae velit tincidunt, ut volutpat justo aliquam. Aenean vitae vehicula arcu. Interdum et malesuada fames ac ante ipsum primis in faucibus. Nunc viverra enim nec risus mollis elementum nec dictum ex. Nunc lorem eros, vulputate a rutrum nec, scelerisque non augue. Sed in egestas eros. Quisque felis lorem, vehicula ac venenatis vel, tristique id sapien. Morbi vitae odio eget orci facilisis suscipit. Cras sodales, augue vitae tincidunt tempus, diam turpis volutpat est, vitae fringilla augue leo semper augue. Integer scelerisque tempor mauris, ac posuere sem aenean"
	colValuePrice       = "150.99"
	colValueStock       = "35"
)

// setUpTmpCsvFile creates a CSV file in the OS's temp directory, like `/tmp/bigcsvreader_<noOfRows>-<randString>.csv` .
// The file will have the provided number of rows.
// Rows look like:
//
//	1,Product_1,"Lorem ipsum...",150.99,35\n
//	2,Product_2,"Lorem ipsum...",150.99,35\n
//	<idIncremented>, Product_<id>, static text: Lorem ipsum..., static price: 150.99, static stock qty: 35 EOL
func setUpTmpCsvFile(rowsCount int64) (string, error) {
	filePattern := "bigcsvreader_" + strconv.FormatInt(rowsCount, 10) + "-*.csv"
	f, err := os.CreateTemp("", filePattern)
	if err != nil {
		return "", err
	}
	fName := f.Name()

	var id int64
	var buf = make([]byte, 0, 1280)
	bufLenConst := 4 + 2 + 1 + len(colValueNamePrefix) + len(colValueDescription) + len(colValuePrice) + len(colValueStock) // 4 x comma, 2 x quote, 1 x \n,
	for id = 1; id <= rowsCount; id++ {
		buf = buf[0:0:1280]
		idStr := strconv.FormatInt(id, 10)
		buf = append(buf, idStr...)
		buf = append(buf, ',')
		buf = append(buf, colValueNamePrefix...)
		buf = append(buf, idStr...)
		buf = append(buf, `,"`...)
		buf = append(buf, colValueDescription...)
		buf = append(buf, `",`...)
		buf = append(buf, colValuePrice...)
		buf = append(buf, ',')
		buf = append(buf, colValueStock...)
		buf = append(buf, "\n"...)
		bufLen := bufLenConst + 2*len(idStr)
		_, err := f.Write(buf[0:bufLen])
		if err != nil {
			_ = f.Close()
			tearDownTmpCsvFile(fName)

			return "", err
		}
	}

	_ = f.Close()

	return fName, nil
}

// tearDownTmpCsvFile deletes the file provided.
func tearDownTmpCsvFile(filePath string) {
	_ = os.Remove(filePath)
}

// fakeProcessRow simulates the processing a row from the CSV file.
// normally record should be validated / converted to a struct / saved into a db / sent over an API...
// here simulates an operation with the cost of 1ms.
func fakeProcessRow(_ []string) {
	time.Sleep(time.Millisecond)
}

func benchmarkBigCsvReader(rowsCount int64) func(b *testing.B) {
	return func(b *testing.B) {
		fName, err := setUpTmpCsvFile(rowsCount)
		if err != nil {
			b.Fatalf("prerequisite failed: could not generate CSV file: %v", err)
		}
		defer tearDownTmpCsvFile(fName)
		subject := bigcsvreader.New()
		subject.SetFilePath(fName)
		subject.ColumnsCount = 5
		subject.MaxGoroutinesNo = 8
		ctx, cancelCtx := context.WithCancel(context.Background())
		defer cancelCtx()
		var count int64

		b.ReportAllocs()
		b.ResetTimer()

		for n := 0; n < b.N; n++ {
			rowsChans, errsChan := subject.Read(ctx)
			count = consumeBenchResults(rowsChans, errsChan)
			if count != rowsCount {
				b.Errorf("expected %d, but got %d", rowsCount, count)
			}
		}
	}
}

// consumeBenchResults just counts the records received from big csv reader and applies a delay of 1ms.
func consumeBenchResults(rowsChans []bigcsvreader.RowsChan, _ bigcsvreader.ErrsChan) int64 {
	var (
		count int64
		wg    sync.WaitGroup
	)

	for i := 0; i < len(rowsChans); i++ {
		wg.Add(1)
		go func(rowsChan bigcsvreader.RowsChan, waitGr *sync.WaitGroup) {
			var localCount int64
			for record := range rowsChan {
				localCount++
				fakeProcessRow(record)
			}
			atomic.AddInt64(&count, localCount)
			waitGr.Done()
		}(rowsChans[i], &wg)
	}
	wg.Wait()

	return count
}

func benchmarkGoCsvReaderReadAll(rowsCount int64) func(b *testing.B) {
	return func(b *testing.B) {
		fName, err := setUpTmpCsvFile(rowsCount)
		if err != nil {
			b.Fatalf("prerequisite failed: could not generate CSV file: %v", err)
		}
		defer tearDownTmpCsvFile(fName)
		var count int64

		b.ReportAllocs()
		b.ResetTimer()

		for n := 0; n < b.N; n++ {
			f, err := os.Open(fName)
			if err != nil {
				b.Fatal(err)
			}
			subject := csv.NewReader(f)
			subject.FieldsPerRecord = 5
			subject.Comma = ','
			count = 0
			rows, err := subject.ReadAll()
			if err != nil {
				b.Error(err)
			} else {
				for _, record := range rows { // "consume" rows
					count++
					fakeProcessRow(record)
				}
			}
			_ = f.Close()
			if count != rowsCount {
				b.Errorf("expected %d, but got %d", rowsCount, count)
			}
		}
	}
}

func benchmarkGoCsvReaderReadOneByOneWithReuseRecord(rowsCount int64) func(b *testing.B) {
	return func(b *testing.B) {
		fName, err := setUpTmpCsvFile(rowsCount)
		if err != nil {
			b.Fatalf("prerequisite failed: could not generate CSV file: %v", err)
		}
		defer tearDownTmpCsvFile(fName)
		var count int64

		b.ReportAllocs()
		b.ResetTimer()

		for n := 0; n < b.N; n++ {
			f, err := os.Open(fName)
			if err != nil {
				b.Fatal(err)
			}
			subject := csv.NewReader(f)
			subject.FieldsPerRecord = 5
			subject.Comma = ','
			subject.ReuseRecord = true
			count = 0

			for {
				record, err := subject.Read()
				if err != nil {
					if err == io.EOF {
						break
					}
					b.Error(err)
				} else { // "consume" row
					count++
					fakeProcessRow(record)
				}
			}
			_ = f.Close()
			if count != rowsCount {
				b.Errorf("expected %d, but got %d", rowsCount, count)
			}
		}
	}
}

func benchmarkGoCsvReaderReadOneByOneProcessParalell(rowsCount int64) func(b *testing.B) {
	return func(b *testing.B) {
		fName, err := setUpTmpCsvFile(rowsCount)
		if err != nil {
			b.Fatalf("prerequisite failed: could not generate CSV file: %v", err)
		}
		defer tearDownTmpCsvFile(fName)

		numWorkers := runtime.GOMAXPROCS(0)

		b.ReportAllocs()
		b.ResetTimer()

		for n := 0; n < b.N; n++ {
			// setup workers for parallel processing
			rowsChan := make(chan []string, numWorkers)
			var (
				count int64
				wg    sync.WaitGroup
			)
			for i := 0; i < numWorkers; i++ {
				wg.Add(1)
				go func() {
					var localCount int64
					for record := range rowsChan {
						localCount++
						fakeProcessRow(record)
					}
					atomic.AddInt64(&count, localCount)
					wg.Done()
				}()
			}

			// sequential reading
			f, err := os.Open(fName)
			if err != nil {
				b.Fatal(err)
			}
			subject := csv.NewReader(f)
			subject.FieldsPerRecord = 5
			subject.Comma = ','
			count = 0

			for {
				record, err := subject.Read()
				if err != nil {
					if err == io.EOF {
						break
					}
					b.Error(err)
				} else { // "consume" row
					rowsChan <- record
				}
			}
			close(rowsChan)
			wg.Wait()
			_ = f.Close()
			if count != rowsCount {
				b.Errorf("expected %d, but got %d", rowsCount, count)
			}
		}
	}
}
func Benchmark50000Rows_50Mb_withBigCsvReader(b *testing.B) {
	benchmarkBigCsvReader(5e4)(b)
}

func Benchmark50000Rows_50Mb_withGoCsvReaderReadAll(b *testing.B) {
	benchmarkGoCsvReaderReadAll(5e4)(b)
}

func Benchmark50000Rows_50Mb_withGoCsvReaderReadOneByOneAndReuseRecord(b *testing.B) {
	benchmarkGoCsvReaderReadOneByOneWithReuseRecord(5e4)(b)
}

func Benchmark50000Rows_50Mb_withGoCsvReaderReadOneByOneProcessParalell(b *testing.B) {
	benchmarkGoCsvReaderReadOneByOneProcessParalell(5e4)(b)
}
