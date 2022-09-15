// Copyright The ActForGood Authors.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://github.com/actforgood/bigcsvreader/blob/main/LICENSE.

// Package main contains an executable for profiling different strategies of reading a CSV.
// Note: this file is only for dev only.
package main

import (
	"context"
	"encoding/csv"
	"flag"
	"io"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/actforgood/bigcsvreader"
)

var generateProfileFor = flag.String("for", "bigcsvreader", "Generate memory and cpu profile for given case. Can be one of bigcsvreader/gocsvreadall/gocsvreadonebyone.")

const rowsCount = 5e4

func main() {
	flag.Parse()

	// create a file
	fName, err := setUpTmpCsvFile(rowsCount)
	if err != nil {
		log.Fatal("prerequisite failed: could not generate CSV file: ", err)
	}
	defer tearDownTmpCsvFile(fName)

	// enable cpu profiling
	fCPU, err := os.Create("./cpu_" + *generateProfileFor + ".prof")
	if err != nil {
		log.Println("could not create CPU profile: ", err)

		return
	}
	defer fCPU.Close()
	if err := pprof.StartCPUProfile(fCPU); err != nil {
		log.Println("could not start CPU profile: ", err)

		return
	}
	defer pprof.StopCPUProfile()

	switch *generateProfileFor {
	case "gocsvreadall":
		goStandardCsvReaderReadAll(fName)
	case "gocsvreadonebyone":
		goStandardCsvReaderReadOneByOne(fName)
	default:
		bigCsvReader(fName)
	}

	// enable memory profiling
	fMem, err := os.Create("./mem_" + *generateProfileFor + ".prof")
	if err != nil {
		log.Println("could not create memory profile: ", err)

		return
	}
	defer fMem.Close()
	runtime.GC() // get up-to-date statistics
	if err := pprof.WriteHeapProfile(fMem); err != nil {
		log.Println("could not write memory profile: ", err)

		return
	}
}

// setUpTmpCsvFile creates a CSV file in the OS's temp directory, like `/tmp/bigcsvreder_<noOfRows>-<randString>.csv` .
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

const (
	colValueNamePrefix  = "Product_"
	colValueDescription = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nunc eleifend felis quis magna auctor, ut lacinia eros efficitur. Maecenas mattis dolor a pharetra gravida. Aenean at eros sed metus posuere feugiat in vitae libero. Morbi a diam volutpat, tempor lacus sed, sagittis velit. Donec eget dignissim mauris, sed aliquam ex. Duis eros dolor, vestibulum ac aliquam eget, viverra in enim. Aenean ut turpis quis purus porta lobortis. Etiam sollicitudin lectus vitae velit tincidunt, ut volutpat justo aliquam. Aenean vitae vehicula arcu. Interdum et malesuada fames ac ante ipsum primis in faucibus. Nunc viverra enim nec risus mollis elementum nec dictum ex. Nunc lorem eros, vulputate a rutrum nec, scelerisque non augue. Sed in egestas eros. Quisque felis lorem, vehicula ac venenatis vel, tristique id sapien. Morbi vitae odio eget orci facilisis suscipit. Cras sodales, augue vitae tincidunt tempus, diam turpis volutpat est, vitae fringilla augue leo semper augue. Integer scelerisque tempor mauris, ac posuere sem aenean"
	colValuePrice       = "150.99"
	colValueStock       = "35"
)

func goStandardCsvReaderReadOneByOne(fName string) {
	var count int64
	f, err := os.Open(fName)
	if err != nil {
		log.Fatal("could not open CSV file", err)
	}
	defer f.Close()
	subject := csv.NewReader(f)
	subject.FieldsPerRecord = 5
	subject.ReuseRecord = true
	subject.Comma = ','
	for {
		record, err := subject.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Println("Read error: ", err)
		} else { // "consume" row
			count++
			_ = record
		}
	}
	log.Println("Rows Count: ", count)
}

func goStandardCsvReaderReadAll(fName string) {
	var count int64
	f, err := os.Open(fName)
	if err != nil {
		log.Fatal("could not open CSV file", err)
	}
	defer f.Close()
	subject := csv.NewReader(f)
	subject.FieldsPerRecord = 5
	subject.Comma = ','
	rows, err := subject.ReadAll()
	if err != nil {
		log.Println("ReadAll error: ", err)
	} else {
		for _, record := range rows { // "consume" rows
			count++
			_ = record
		}
	}
	log.Println("Rows Count: ", count)
}

func bigCsvReader(fName string) {
	subject := bigcsvreader.New()
	subject.SetFilePath(fName)
	subject.ColumnsCount = 5
	subject.MaxGoroutinesNo = 8
	ctx, cancelCtx := context.WithCancel(context.Background())
	defer cancelCtx()
	var count int64

	rowsChans, errsChan := subject.Read(ctx)
	count = consumeBigCsvReaderResults(rowsChans, errsChan)
	log.Println("Rows Count: ", count)
}

// consumeBigCsvReaderResults just counts the records received from big csv reader.
func consumeBigCsvReaderResults(rowsChans []bigcsvreader.RowsChan, errsChan bigcsvreader.ErrsChan) int64 {
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
				_ = record
			}
			atomic.AddInt64(&count, localCount)
			waitGr.Done()
		}(rowsChans[i], &wg)
	}

	wg.Add(1)
	go func(errsCh bigcsvreader.ErrsChan, waitGr *sync.WaitGroup) {
		for err := range errsCh {
			log.Println("Read error: ", err)
		}
		waitGr.Done()
	}(errsChan, &wg)

	wg.Wait()

	return count
}
