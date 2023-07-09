package row

import (
	"db/src/storage"
)

type Row interface {
	Save(index uint) (n int, err error)
	Load() (err error)
	Table() *storage.Table
	InitCursor(index uint)
}

type emptyRow struct {
	Row
	Cursor    *cursor
	TableName string
}

func (row *emptyRow) InitCursor(index uint) {
	t := storage.InitTable(row.TableName)
	row.Cursor = &cursor{
		table:      t,
		isEnd:      t.RowNum == index,
		currentRow: index,
	}
}

func (row *emptyRow) Table() *storage.Table {
	return row.Cursor.table
}
