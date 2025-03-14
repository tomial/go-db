package btree

import (
	"encoding/hex"
	"testing"

	"github.com/tomial/go-db/internal/constants"
)

func initInternalNode() *InternalNode {
	in := &InternalNode{Header: initNodeHeader()}
	in.Header.NumCell = 3
	in.Header.Typ = TypeInternal
	in.Header.CellSize = internalNodeCellSize()
	in.Cells = []*internalCell{
		{
			key:   2,
			left:  3,
			right: 4,
		},
		{
			key:   5,
			left:  4,
			right: 6,
		},
		{
			key:   7,
			left:  6,
			right: 8,
		},
	}
	return in
}

func TestInternalNodeCellSize(t *testing.T) {
	in := initInternalNode()
	in.Header.CellSize = internalNodeCellSize()
	expected := 12
	if in.Header.CellSize != uint32(expected) {
		t.Fatalf("Wrong internal node cell size: %d, expected %d", in.Header.CellSize, expected)
	}
}

func TestMaxInternalNodeNumCell(t *testing.T) {
	size := maxInternalNodeNumCell()
	// body size / internal node size == 340
	// limited amount here, 340 is too large
	expected := 3
	if size != uint32(expected) {
		t.Fatalf("Wrong internal node cell size: %d, expected %d", size, expected)
	}
}

func TestMakeInternalNodeEmptyPage(t *testing.T) {
	buf := makeNodePage(constants.MagicNumberInternal)
	magicNumberStr := hex.EncodeToString(buf[:constants.MagicNumberSize])
	if magicNumberStr != constants.MagicNumberInternal || magicNumberStr == constants.MagicNumberLeaf {
		t.Fatalf("Failed to make leaf page: %s, expected %s\n", magicNumberStr, constants.MagicNumberInternal)
	}
}

func TestInternalNodeSerializeCells(t *testing.T) {
	in := initInternalNode()
	in.Cells = []*internalCell{
		{
			key:   2,
			left:  3,
			right: 4,
		},
		{
			key:   5,
			left:  4,
			right: 6,
		},
		{
			key:   7,
			left:  6,
			right: 8,
		},
	}
	cellsBytes, err := in.serializeCells()
	if err != nil {
		t.Error(err)
	}

	in.Cells = nil
	in.deserializeCells(cellsBytes)
	if in.Cells[0].key != 2 || in.Cells[0].left != 3 || in.Cells[0].right != 4 ||
		in.Cells[1].key != 5 || in.Cells[1].left != 4 || in.Cells[1].right != 6 {
		t.Error("Failed to serialize and deserialize internal node cells")
	}
}

func TestInternalNodeSerialization(t *testing.T) {
	in := initInternalNode()
	bytes := in.serialize()
	in1 := &InternalNode{Header: &nodeHeader{}}
	err := in1.deserialize(bytes)
	if err != nil {
		t.Error(err)
	}
	if in.Header.Typ != in1.Header.Typ ||
		in.Header.Height != in1.Header.Height ||
		in.Header.Next != in1.Header.Next ||
		in.Header.NumCell != in1.Header.NumCell ||
		in.Header.Parent != in1.Header.Parent ||
		in.Header.CellSize != in1.Header.CellSize ||
		in.Header.Page != in1.Header.Page ||
		in.Cells[0].key != in1.Cells[0].key ||
		in.Cells[0].left != in1.Cells[0].left ||
		in.Cells[0].right != in1.Cells[0].right ||
		in.Cells[1].key != in1.Cells[1].key ||
		in.Cells[1].left != in1.Cells[1].left ||
		in.Cells[1].right != in1.Cells[1].right {
		t.Fatal("Testing internal node serialization: cannot serialize and deserialize internal node correctly")
	}
}
