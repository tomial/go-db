package btree

import "testing"

func TestInternalNodeCellSize(t *testing.T) {
	in := &InternalNode{}
	in.CellSize = internalNodeCellSize()
	expected := 12
	if in.CellSize != uint32(expected) {
		t.Fatalf("Wrong internal node cell size: %d, expected %d", in.CellSize, expected)
	}
}

func TestMaxInternalNodeNumCell(t *testing.T) {
	size := maxInternalNodeNumCell()
	// 4082 / 12 == 340
	// limited amount
	expected := 2
	if size != uint32(expected) {
		t.Fatalf("Wrong internal node cell size: %d, expected %d", size, expected)
	}
}
