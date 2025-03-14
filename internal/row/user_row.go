package row

import (
	"log"
	"reflect"

	"github.com/tomial/go-db/internal/datatype"
)

type UserRow struct {
	emptyRow
	Id       uint64
	Username string
	Email    string
}

func (row *UserRow) Save(index uint32) (n int, err error) {
	bytes, err := serialize(row, datatype.Uint64Size+2*datatype.StringSize)
	if err != nil {
		return 0, err
	}
	if row.Cursor == nil {
		row.InitCursor(index)
	}
	err = row.Cursor.table.Persist(bytes, row.Cursor.currentPos())
	if err != nil {
		log.Println(err.Error())
		return 0, nil
	} else {
		row.Cursor.advance()
		return len(bytes), nil
	}
}

func (row *UserRow) Load() (err error) {
	data, err := row.Cursor.table.Load(row.Cursor.currentPos())
	if err != nil {
		return err
	}

	rowType := reflect.TypeOf(*row)
	loadedRow, err := deserialize(data, rowType)
	if err != nil {
		return err
	}

	ptr := (loadedRow).(*UserRow)

	log.Printf("Loaded [ ID #%d UserRow: Username-> %s, Email-> %s ]\n", ptr.Id, ptr.Username, ptr.Email)

	return nil
}
