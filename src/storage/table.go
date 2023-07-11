package storage

import (
	"db/src/constants"
	"db/src/datatype"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
)

type Table struct {
	Name    string
	Page    Pager
	RowNum  uint
	RowSize uint
}

func (t *Table) Persist(data []byte, slot uint) error {
	writePos := slot * t.RowSize
	if writePos > constants.PageSize {
		return errors.New("persisting data: page full")
	}
	t.Page.File.Seek(int64(writePos), io.SeekStart)

	_, err := t.Page.File.Write(data)
	if err != nil {
		return err
	} else {
		return nil
	}
}

func (t *Table) Load(index uint) ([]byte, error) {
	if index == 0 {
		return nil, errors.New("loading data: invalid index 0")
	}
	loadPos := (index - 1) * t.RowSize
	if loadPos+t.RowSize > constants.PageSize {
		return nil, errors.New("loading data: no data left to be loaded")
	}

	buf := make([]byte, t.RowSize)

	t.Page.File.Seek(int64(loadPos), io.SeekStart)
	_, err := t.Page.File.Read(buf)
	if err != nil {
		return nil, fmt.Errorf("loading data: failed to read from database file --- %s", err.Error())
	}

	return buf, nil
}

func InitTable(name string) *Table {
	file, err := os.OpenFile(constants.DbFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0755)
	if err != nil {
		log.Fatalf("Initializing table: failed to open database file %s -- %s", constants.DbFileName, err)
	}
	fstat, err := file.Stat()
	if err != nil {
		log.Fatalf("Initializing table: failed to read database file stat %s -- %s", constants.DbFileName, err)
	}
	rowSize := datatype.StringSize + datatype.StringSize + datatype.Uint64Size
	return &Table{
		Name:    name,
		Page:    Pager{File: file},
		RowNum:  uint(fstat.Size()) / rowSize,
		RowSize: rowSize,
	}
}

func (t *Table) String() string {
	return t.Name
}
