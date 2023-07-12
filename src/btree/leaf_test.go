package btree

import (
	"db/src/storage"
	"testing"
)

func TestLeafNodeCellSize(t *testing.T) {
	lf := &LeafNode{}
	table := storage.Table{RowSize: 520}
	lf.setCellSize(table.RowSize)
	size := lf.CellSize
	expected := 524 // 4 + table row size
	if size != uint32(expected) {
		t.Fatalf("Wrong leaf node cell size: %d, expected %d", size, expected)
	}
}

func TestMaxLeafNodeNumCell(t *testing.T) {
	lf := &LeafNode{}
	table := storage.Table{RowSize: 520}
	lf.setCellSize(table.RowSize)
	maxNum := maxLeafNodeNumCell(lf) // 4080 / 524 == 7
	expected := 7
	if maxNum != uint32(expected) {
		t.Fatalf("Wrong max leaf node cell num: %d, expected %d", maxNum, expected)
	}
}
