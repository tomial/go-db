package btree

import (
	"db/src/constants"
	"db/src/storage"
	"encoding/hex"
	"testing"
)

func TestLeafNodeCellSize(t *testing.T) {
	lf := &LeafNode{}
	table := storage.Table{RowSize: 520}
	lf.setCellSize(table.RowSize)
	size := lf.CellSize
	expected := 524 // key + table row size
	if size != uint32(expected) {
		t.Fatalf("Wrong leaf node cell size: %d, expected %d", size, expected)
	}
}

func TestMaxLeafNodeNumCell(t *testing.T) {
	lf := &LeafNode{}
	table := storage.Table{RowSize: 520}
	lf.setCellSize(table.RowSize)
	maxNum := lf.maxLeafNodeNumCell()
	expected := 7
	if maxNum != uint32(expected) {
		t.Fatalf("Wrong max leaf node cell num: %d, expected %d", maxNum, expected)
	}
}

func TestMakeLeafNodeEmptyPage(t *testing.T) {
	buf := makeNodePage(constants.MagicNumberLeaf)
	magicNumberStr := hex.EncodeToString(buf[:constants.MagicNumberSize])
	if magicNumberStr != constants.MagicNumberLeaf || magicNumberStr == constants.MagicNumberInternal {
		t.Fatalf("Failed to make leaf page: %s, expected %s\n", magicNumberStr, constants.MagicNumberLeaf)
	}
}
