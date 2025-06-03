// Copyright The ActForGood Authors.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://github.com/actforgood/bigcsvreader/blob/main/LICENSE.

package bigcsvreader_test

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/actforgood/bigcsvreader"
)

const (
	columnProductID = iota
	columnProductName
	columnProductDescription
	columnProductPrice
	columnProductQty
)

const noOfColumns = 5

type Product struct {
	ID    int
	Name  string
	Desc  string
	Price float64
	Qty   int
}

func ExampleCsvReader() {
	// initialize the big csv reader
	bigCSV := bigcsvreader.New()
	bigCSV.SetFilePath("testdata/example_products.csv")
	bigCSV.ColumnsCount = noOfColumns
	bigCSV.MaxGoroutinesNo = 16

	ctx, cancelCtx := context.WithCancel(context.Background())
	defer cancelCtx()
	var wg sync.WaitGroup

	// start multi-thread reading
	rowsChans, errsChan := bigCSV.Read(ctx)

	// process rows and errors:

	for i := range rowsChans {
		wg.Add(1)
		go rowWorker(rowsChans[i], &wg)
	}

	wg.Add(1)
	go errWorker(errsChan, &wg)

	wg.Wait()

	// Unordered output:
	// {ID:1 Name:Apple iPhone 13 Desc:Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nunc eleifend felis quis magna auctor, ut lacinia eros efficitur. Maecenas mattis dolor a pharetra gravida. Aenean at eros sed metus posuere feugiat in vitae libero. Morbi a diam volutpat, tempor lacus sed, sagittis velit. Donec eget dignissim mauris, sed aliquam ex. Duis eros dolor, vestibulum ac aliquam eget, viverra in enim. Aenean ut turpis quis purus porta lobortis. Etiam sollicitudin lectus vitae velit tincidunt, ut volutpat justo aliquam. Aenean vitae vehicula arcu. Interdum et malesuada fames ac ante ipsum primis in faucibus. Nunc viverra enim nec risus mollis elementum nec dictum ex. Nunc lorem eros, vulputate a rutrum nec, scelerisque non augue. Sed in egestas eros. Quisque felis lorem, vehicula ac venenatis vel, tristique id sapien. Morbi vitae odio eget orci facilisis suscipit. Cras sodales, augue vitae tincidunt tempus, diam turpis volutpat est, vitae fringilla augue leo semper augue. Integer scelerisque tempor mauris, ac posuere sem aenean Price:1025.99 Qty:100}
	// {ID:2 Name:Samsung Galaxy S22 Desc:Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nunc eleifend felis quis magna auctor, ut lacinia eros efficitur. Maecenas mattis dolor a pharetra gravida. Aenean at eros sed metus posuere feugiat in vitae libero. Morbi a diam volutpat, tempor lacus sed, sagittis velit. Donec eget dignissim mauris, sed aliquam ex. Duis eros dolor, vestibulum ac aliquam eget, viverra in enim. Aenean ut turpis quis purus porta lobortis. Etiam sollicitudin lectus vitae velit tincidunt, ut volutpat justo aliquam. Aenean vitae vehicula arcu. Interdum et malesuada fames ac ante ipsum primis in faucibus. Nunc viverra enim nec risus mollis elementum nec dictum ex. Nunc lorem eros, vulputate a rutrum nec, scelerisque non augue. Sed in egestas eros. Quisque felis lorem, vehicula ac venenatis vel, tristique id sapien. Morbi vitae odio eget orci facilisis suscipit. Cras sodales, augue vitae tincidunt tempus, diam turpis volutpat est, vitae fringilla augue leo semper augue. Integer scelerisque tempor mauris, ac posuere sem aenean Price:400.99 Qty:12}
	// {ID:3 Name:Apple MacBook Air Desc:Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nunc eleifend felis quis magna auctor, ut lacinia eros efficitur. Maecenas mattis dolor a pharetra gravida. Aenean at eros sed metus posuere feugiat in vitae libero. Morbi a diam volutpat, tempor lacus sed, sagittis velit. Donec eget dignissim mauris, sed aliquam ex. Duis eros dolor, vestibulum ac aliquam eget, viverra in enim. Aenean ut turpis quis purus porta lobortis. Etiam sollicitudin lectus vitae velit tincidunt, ut volutpat justo aliquam. Aenean vitae vehicula arcu. Interdum et malesuada fames ac ante ipsum primis in faucibus. Nunc viverra enim nec risus mollis elementum nec dictum ex. Nunc lorem eros, vulputate a rutrum nec, scelerisque non augue. Sed in egestas eros. Quisque felis lorem, vehicula ac venenatis vel, tristique id sapien. Morbi vitae odio eget orci facilisis suscipit. Cras sodales, augue vitae tincidunt tempus, diam turpis volutpat est, vitae fringilla augue leo semper augue. Integer scelerisque tempor mauris, ac posuere sem aenean Price:700.99 Qty:34}
	// {ID:4 Name:Lenovo ThinkPad X1 Desc:Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nunc eleifend felis quis magna auctor, ut lacinia eros efficitur. Maecenas mattis dolor a pharetra gravida. Aenean at eros sed metus posuere feugiat in vitae libero. Morbi a diam volutpat, tempor lacus sed, sagittis velit. Donec eget dignissim mauris, sed aliquam ex. Duis eros dolor, vestibulum ac aliquam eget, viverra in enim. Aenean ut turpis quis purus porta lobortis. Etiam sollicitudin lectus vitae velit tincidunt, ut volutpat justo aliquam. Aenean vitae vehicula arcu. Interdum et malesuada fames ac ante ipsum primis in faucibus. Nunc viverra enim nec risus mollis elementum nec dictum ex. Nunc lorem eros, vulputate a rutrum nec, scelerisque non augue. Sed in egestas eros. Quisque felis lorem, vehicula ac venenatis vel, tristique id sapien. Morbi vitae odio eget orci facilisis suscipit. Cras sodales, augue vitae tincidunt tempus, diam turpis volutpat est, vitae fringilla augue leo semper augue. Integer scelerisque tempor mauris, ac posuere sem aenean Price:550.99 Qty:90}
	// {ID:5 Name:Logitech Mouse G203 Desc:Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nunc eleifend felis quis magna auctor, ut lacinia eros efficitur. Maecenas mattis dolor a pharetra gravida. Aenean at eros sed metus posuere feugiat in vitae libero. Morbi a diam volutpat, tempor lacus sed, sagittis velit. Donec eget dignissim mauris, sed aliquam ex. Duis eros dolor, vestibulum ac aliquam eget, viverra in enim. Aenean ut turpis quis purus porta lobortis. Etiam sollicitudin lectus vitae velit tincidunt, ut volutpat justo aliquam. Aenean vitae vehicula arcu. Interdum et malesuada fames ac ante ipsum primis in faucibus. Nunc viverra enim nec risus mollis elementum nec dictum ex. Nunc lorem eros, vulputate a rutrum nec, scelerisque non augue. Sed in egestas eros. Quisque felis lorem, vehicula ac venenatis vel, tristique id sapien. Morbi vitae odio eget orci facilisis suscipit. Cras sodales, augue vitae tincidunt tempus, diam turpis volutpat est, vitae fringilla augue leo semper augue. Integer scelerisque tempor mauris, ac posuere sem aenean Price:30.5 Qty:35}
}

func rowWorker(rowsChan bigcsvreader.RowsChan, waitGr *sync.WaitGroup) {
	for row := range rowsChan {
		processRow(row)
	}
	waitGr.Done()
}

func errWorker(errsChan bigcsvreader.ErrsChan, waitGr *sync.WaitGroup) {
	for err := range errsChan {
		handleError(err)
	}
	waitGr.Done()
}

// processRow can be used to implement business logic
// like validation / converting to a struct / persisting row into a storage.
func processRow(row []string) {
	id, _ := strconv.Atoi(row[columnProductID])
	price, _ := strconv.ParseFloat(row[columnProductPrice], 64)
	qty, _ := strconv.Atoi(row[columnProductQty])
	name := row[columnProductName]
	desc := row[columnProductDescription]

	product := Product{
		ID:    id,
		Name:  name,
		Desc:  desc,
		Price: price,
		Qty:   qty,
	}

	fmt.Printf("%+v\n", product)
}

// handleError handles the error.
// errors can be fatal like file does not exist, or row related like a given row could not be parsed, etc...
func handleError(err error) {
	fmt.Println(err)
}
