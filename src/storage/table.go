package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

type table struct {
	name    string
	page    pager
	rowNum  uint
	rowSize uint
}

const dataTypeStringSize uint = 255
const dataTypeInt64Size uint = binary.MaxVarintLen64
const dataTypeUint64Size uint = binary.MaxVarintLen64

var dataTypeSize = map[string]uint{
	"string": dataTypeStringSize,
	"int":    dataTypeInt64Size,
	"uint":   dataTypeUint64Size,
}

func (t *table) persist(data []byte) error {
	writePos := t.rowNum * t.rowSize
	if writePos+t.rowSize > pageSize {
		return errors.New("persisting data: page full")
	}
	t.page.file.Seek(int64(writePos), io.SeekStart)

	_, err := t.page.file.Write(data)
	if err != nil {
		return err
	} else {
		return nil
	}
}

func (t *table) load(index uint) ([]byte, error) {
	if index == 0 {
		index = 1
	}
	loadPos := (index - 1) * t.rowSize
	if loadPos+t.rowSize > pageSize {
		return nil, errors.New("loading data: no data left to be loaded")
	}

	buf := make([]byte, t.rowSize)

	t.page.file.Seek(int64(loadPos), io.SeekStart)
	_, err := t.page.file.Read(buf)
	if err != nil {
		return nil, fmt.Errorf("loading data: failed to read from database file --- %s", err.Error())
	}

	return buf, nil
}
