package storage

import (
	"fmt"
	"log"
	"os"
	"reflect"
)

type Row interface {
	Save() (n int, err error)
	Load(index uint) (err error)
	Table() string
	initTable()
}

type emptyRow struct {
	Row
	table *table
}

type UserRow struct {
	emptyRow
	Id       uint64
	Username string
	Email    string
}

func (row *UserRow) initTable() {
	if row.table == nil {
		file, err := os.OpenFile(dbFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0755)
		if err != nil {
			log.Fatalf("Init table: failed to open database file %s -- %s", dbFileName, err)
		}
		row.table = &table{
			name:    "User",
			page:    pager{file: file},
			rowNum:  0,
			rowSize: dataTypeStringSize + dataTypeStringSize + dataTypeUint64Size,
		}
	}
}

func (row *UserRow) Save() (n int, err error) {
	row.initTable()
	bytes, err := serialize(row, dataTypeUint64Size+2*dataTypeStringSize)
	if err != nil {
		return 0, err
	}
	err = row.table.persist(bytes)
	if err != nil {
		return 0, nil
	} else {
		return len(bytes), nil
	}
}

func (row *UserRow) Load(index uint) (err error) {
	row.initTable()

	data, err := row.table.load(index)
	if err != nil {
		return err
	}

	rowType := reflect.TypeOf(*row)
	loadedRow, err := deserialize(data, rowType)
	if err != nil {
		return err
	}

	ptr := (loadedRow).(*UserRow)

	fmt.Printf("Loaded [ ID #%d UserRow: Username-> %s, Email-> %s ]\n", ptr.Id, ptr.Username, ptr.Email)

	return nil
}

func (row *UserRow) Table() string {
	return row.table.name
}
