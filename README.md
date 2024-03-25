# BigCsvReader

[![Build Status](https://github.com/actforgood/bigcsvreader/actions/workflows/build.yml/badge.svg)](https://github.com/actforgood/bigcsvreader/actions/workflows/build.yml)
[![License](https://img.shields.io/badge/license-MIT-blue)](https://raw.githubusercontent.com/actforgood/bigcsvreader/main/LICENSE)
[![Coverage Status](https://coveralls.io/repos/github/actforgood/bigcsvreader/badge.svg?branch=main)](https://coveralls.io/github/actforgood/bigcsvreader?branch=main)
[![Goreportcard](https://goreportcard.com/badge/github.com/actforgood/bigcsvreader)](https://goreportcard.com/report/github.com/actforgood/bigcsvreader)
[![Go Reference](https://pkg.go.dev/badge/github.com/actforgood/bigcsvreader.svg)](https://pkg.go.dev/github.com/actforgood/bigcsvreader)  

---  

Package `bigcscvreader` offers a multi-threaded approach for reading a large CSV file in order to improve the time of reading and processing it.  
It spawns multiple goroutines, each reading a piece of the file.  
Read rows are put into channels equal in number to the spawned goroutines, in this way also the processing of those rows can be parallelized.  


### Installation

```shell
$ go get github.com/actforgood/bigcsvreader
```

### Example

Please refer to this [example](https://pkg.go.dev/github.com/actforgood/bigcsvreader#example-CsvReader).


### How it is designed to work
![BigCsvReader-HowItWorks](docs/how-it-works.svg)


### Benchmarks
```
go version go1.22.1 darwin/amd64
go test -timeout=15m -benchmem -benchtime=2x -bench . 
goos: darwin
goarch: amd64
pkg: github.com/actforgood/bigcsvreader
cpu: Intel(R) Core(TM) i7-7700HQ CPU @ 2.80GHz
Benchmark50000Rows_50Mb_withBigCsvReader-8                                 2    8076491568 ns/op     61744680 B/op    100269 allocs/op
Benchmark50000Rows_50Mb_withStdGoCsvReaderReadAll-8   	                   2    65237799108 ns/op    67924264 B/op    100043 allocs/op
Benchmark50000Rows_50Mb_withStdGoCsvReaderReadOneByOneAndReuseRecord-8     2    66750849960 ns/op    57606432 B/op     50020 allocs/op
Benchmark50000Rows_50Mb_withStdGoCsvReaderReadOneByOneProcessParalell-8    2    8184433872 ns/op     61607624 B/op    100040 allocs/op
```

Benchmarks are made with a file of ~`50Mb` in size, also a fake processing of any given row of `1ms` was taken into consideration.  
bigcsvreader was launched with `8` goroutines.  
Other benchmarks are made using directly the `encoding/csv` go package.  
As you can see, bigcsvreader reads and processes all rows in ~`8s`.  
Go standard csv package reads and processes all rows in ~`65s` (sequentially).  
Go standard csv package read and a parallel processing of rows timing is comparable to the one of bigcsvreader (so this strategy is a good alternative to this package).  
`ReadAll` API has the disadvantage of keeping all rows into memory.  
`Read` rows one by one API with `ReuseRecord` flag set has the advantage of fewer allocations, but has the cost of sequentially reading rows.  
> Note: It's a coincidence that parallelized version timing was ~equal to sequential timing divided by no of started goroutines. You should not take this as a rule.

Bellow are some process stats captured with unix `TOP` command while running each benchmark.
| Bench | %CPU | MEM |
| --- | --- | --- |
| Benchmark50000Rows_50Mb_withBigCsvReader | 17.3 | 9652K |
| Benchmark50000Rows_50Mb_withStdGoCsvReaderReadAll | 5.8 | 66M |
| Benchmark50000Rows_50Mb_withStdGoCsvReaderReadOneByOneAndReuseRecord | 11.3 | 6908K |


**(!) Known issue**:
This package does not work as expected with multiline columns.


### License
This package is released under a MIT license. See [LICENSE](LICENSE).  
