package row

import "github.com/tomial/go-db/internal/storage"

type cursor struct {
	table      *storage.Table
	isEnd      bool
	currentRow uint32
}

// Move to start
func (c *cursor) tableStart() {
	c.currentRow = 1
}

// Move to the last row
func (c *cursor) tableEnd() {
	c.currentRow = c.table.RowNum
}

func (c *cursor) currentPos() uint32 {
	return c.currentRow
}

// Move to next position for inserting
func (c *cursor) advance() {
	c.currentRow += 1
}
