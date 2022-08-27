// Copyright 2022 Bogdan Constantinescu.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://github.com/actforgood/bigcsvreader/blob/main/LICENSE.

package bigcsvreader

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"io"
	"os"
	"path"
	"runtime"
	"sync"

	"github.com/actforgood/bigcsvreader/internal"
)

const (
	chanSize                   = 256
	minBytesToReadByAGoroutine = 2048
)

// ErrEmptyFile is an error returned if CSV file is empty.
var ErrEmptyFile = errors.New("empty csv file")

// RowsChan is the channel where read rows will be pushed into.
// Has a buffer of 256 entries.
type RowsChan <-chan []string

// ErrsChan is the channel where error(s) will be pushed in case
// an error occurs during file read. Has a buffer of 256 entries.
// Some errors can be fatal, like file does not exist, some errors like
// rows parsing may occur for each affected row.
type ErrsChan <-chan error

// CsvReader reads async rows from a CSV file.
// It does that by initializing multiple goroutines, each of them handling
// a chunk of data from the file.
type CsvReader struct {
	// MaxGoroutinesNo is the maximum goroutines to start parsing the CSV file.
	// Minimum required bytes to start a new goroutine is 2048 bytes.
	// Defaults to `runtime.NumCPU()`.
	MaxGoroutinesNo int
	// FileHasHeader is a flag indicating if file's first row is the header (columns names).
	// If so, the header line is disregarded and not returned as a row.
	// Defaults to false.
	FileHasHeader bool
	// ColumnsCount is the number of columns the CSV file has.
	ColumnsCount int
	// ColumnsDelimiter is the delimiter char between columns. Defaults to comma.
	ColumnsDelimiter rune
	// BufferSize is used internally for `bufio.Reader` size. Has a default value of 4096.
	// If you have lines bigger than this value, adjust it not to get "buffer full" error.
	BufferSize int
	// Logger can be set to perform some debugging/error logging.
	// Defaults to a no-operation logger (no log is performed).
	// You can enable logging by passing a logger that implements `internal.Logger` contract.
	Logger internal.Logger
	// filePath is the CSV file path.
	filePath string
	// fileBaseName is the base name of the file extracted from filePath.
	// Is used in logging.
	fileBaseName string
}

// New instantiates a new CsvReader object with some default fields preset.
func New() *CsvReader {
	return &CsvReader{
		MaxGoroutinesNo:  runtime.NumCPU(),
		ColumnsDelimiter: ',',
		Logger:           internal.NopLogger{},
		BufferSize:       4096,
	}
}

// SetFilePath sets the CSV file path.
func (cr *CsvReader) SetFilePath(csvFilePath string) {
	cr.filePath = csvFilePath
	cr.fileBaseName = path.Base(csvFilePath)
}

// Read extracts asynchronously CSV rows, each started thread putting them into a RowsChan.
// Error(s) occurred during parsing are sent through ErrsChan.
func (cr *CsvReader) Read(ctx context.Context) ([]RowsChan, ErrsChan) {
	cr.Logger.Debug(
		"msg", "starting file reading",
		"filePath", cr.filePath,
		"fileColumnsCount", cr.ColumnsCount,
		"fileHasHeader", cr.FileHasHeader,
		"maxThreads", cr.MaxGoroutinesNo,
	)

	errsChan := make(chan error, chanSize)
	fileSize, err := cr.getFileSize()
	if err != nil {
		errsChan <- err
		close(errsChan)
		cr.Logger.Error(
			"msg", "could not get file size",
			"err", err,
			"file", cr.fileBaseName,
		)

		return nil, errsChan
	}

	threadsInfo := internal.ComputeGoroutineOffsets(fileSize, cr.MaxGoroutinesNo, minBytesToReadByAGoroutine)
	totalThreads := len(threadsInfo)
	cr.Logger.Debug(
		"msg", "stats",
		"file", cr.fileBaseName, "fileSize", fileSize,
		"totalThreads", totalThreads, "initialOffsetsDistribution", threadsInfo,
	)

	rowsChans := make([]RowsChan, totalThreads)
	rowsChs := make([]chan<- []string, totalThreads)
	for i := 0; i < totalThreads; i++ {
		rowsChan := make(chan []string, chanSize)
		rowsChans[i] = rowsChan
		rowsChs[i] = rowsChan
	}

	go cr.readAsync(ctx, threadsInfo, rowsChs, errsChan)

	return rowsChans, errsChan
}

func (cr *CsvReader) readAsync(
	ctx context.Context,
	threadsInfo [][2]int,
	rowsChans []chan<- []string,
	errsChan chan<- error,
) {
	defer func() {
		close(errsChan)
		for i := 0; i < len(rowsChans); i++ {
			close(rowsChans[i])
		}
	}()
	totalThreads := len(threadsInfo)

	// create a wait group pool as we need to wait for all goroutines to terminate.
	var wg sync.WaitGroup
	wg.Add(totalThreads)
	worker := cr.readBetweenOffsetsAsync
	for thread := 0; thread < totalThreads; thread++ {
		go worker(
			ctx,
			thread+1,
			threadsInfo[thread][0], // start offset
			threadsInfo[thread][1], // end offset
			&wg,
			rowsChans[thread],
			errsChan,
		)
	}
	wg.Wait()

	cr.Logger.Debug("msg", "finished file reading", "file", cr.fileBaseName)
}

// readBetweenOffsetsAsync reads the piece of file allocated to a given thread.
func (cr *CsvReader) readBetweenOffsetsAsync(
	ctx context.Context,
	currentThreadNo, offsetStart, offsetEnd int,
	wg *sync.WaitGroup,
	rowsChan chan<- []string,
	errsChan chan<- error,
) {
	defer wg.Done()

	f := cr.openFile(currentThreadNo, errsChan)
	if f == nil {
		return
	}
	defer f.Close()

	var line []byte

	// move offset to startOffset and skip the whole line.
	r := bufio.NewReaderSize(f, cr.BufferSize)
	_, _ = f.Seek(int64(offsetStart), io.SeekStart)
	if currentThreadNo != 1 || cr.FileHasHeader {
		line = cr.readLine(r, currentThreadNo, offsetStart, errsChan)
		if line == nil {
			return
		}
	}
	realOffsetStart := offsetStart + len(line)
	currentOffsetPos := realOffsetStart

	bytesReader := bytes.NewReader(line)
	csvReader := csv.NewReader(bytesReader)
	csvReader.Comma = cr.ColumnsDelimiter
	csvReader.FieldsPerRecord = cr.ColumnsCount

ForLoop:
	for {
		select {
		case <-ctx.Done():
			if ctx.Err() != nil {
				errsChan <- ctx.Err()
			}

			return
		default:
			line = cr.readLine(r, currentThreadNo, currentOffsetPos, errsChan)
			if line == nil {
				break ForLoop
			}

			// pass read line through standard go CSV reader.
			bytesReader.Reset(line)
			record, err := csvReader.Read()
			if err != nil {
				errsChan <- err
				cr.Logger.Error(
					"msg", "could not parse row", "err", err,
					"file", cr.fileBaseName, "thread", currentThreadNo,
					"offset", currentOffsetPos, "row", string(line),
				)
			} else {
				rowsChan <- record
			}

			currentOffsetPos += len(line)
			if currentOffsetPos-1 > offsetEnd {
				break ForLoop // next thread will handle eventual next lines.
			}
		}
	}

	cr.Logger.Debug(
		"msg", "done",
		"file", cr.fileBaseName, "thread", currentThreadNo,
		"offsetStart", offsetStart, "offsetEnd", offsetEnd,
		"realOffsetStart", realOffsetStart, "realOffsetEnd", currentOffsetPos-1,
		"bytesCount", currentOffsetPos-realOffsetStart,
	)
}

// openFile returns the fd of CSV file or nil if the file could not be opened.
func (cr *CsvReader) openFile(thread int, errsChan chan<- error) *os.File {
	f, err := os.Open(cr.filePath)
	if err == nil {
		return f
	}

	errsChan <- err
	cr.Logger.Error(
		"msg", "could not open file", "err", err,
		"file", cr.fileBaseName, "thread", thread,
	)

	return nil
}

// readLine reads returns a row from file, or nil if something bad happens or `io.EOF` is encountered.
func (cr *CsvReader) readLine(r *bufio.Reader, thread, offsetPos int, errsChan chan<- error) []byte {
	// did not use `r.ReadLine` as it disregards end line delimiter(s) (\n / \r\n)
	// and we need the whole line length in advancing offset.
	// `ReadSlice` also returns the subslice of buffered bytes, without allocating another slice.
	line, err := r.ReadSlice('\n')
	if err == nil {
		return line
	}
	if err == io.EOF {
		if len(line) != 0 {
			return line
		}
	} else {
		errsChan <- err
		cr.Logger.Error(
			"msg", "could not read line", "err", err,
			"file", cr.fileBaseName, "thread", thread,
			"offset", offsetPos,
		)
	}

	return nil
}

// getFileSize returns file's size as each goroutine will
// read approx. fileSize/totalGoroutines bytes.
func (cr *CsvReader) getFileSize() (int, error) {
	fileInfo, err := os.Stat(cr.filePath)
	if err != nil {
		return 0, err
	}
	fileSize := int(fileInfo.Size())
	if fileSize < 1 {
		return 0, ErrEmptyFile
	}

	return fileSize, nil
}
