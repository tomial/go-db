package storage

import (
	"db/src/constants"
	"db/src/datatype"
	"db/src/pager"
	"errors"
	"fmt"
	"io"
)

type Table struct {
	Name    string
	Page    *pager.Pager
	RowNum  uint32
	RowSize uint32
}

func (t *Table) Persist(data []byte, slot uint32) error {
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

func (t *Table) Load(index uint32) ([]byte, error) {
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
	rowSize := datatype.StringSize + datatype.StringSize + datatype.Uint64Size
	pager := pager.Init()
	return &Table{
		Name:    name,
		Page:    pager,
		RowNum:  uint32(pager.Fstat().Size()) / rowSize,
		RowSize: rowSize,
	}
}

func (t *Table) String() string {
	return t.Name
}
