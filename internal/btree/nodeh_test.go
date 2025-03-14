package btree

import (
	"testing"
)

func initNodeHeader() *nodeHeader {
	return &nodeHeader{
		Typ:      TypeLeaf,
		Parent:   1,
		Next:     3,
		CellSize: 542,
		Page:     1,
		Height:   1,
		NumCell:  4,
	}
}

func TestNodeHeaderSize(t *testing.T) {
	size := nodeHeaderSize()
	var expected uint32 = 19
	if size != expected {
		t.Fatalf("Wrong node header size: %d, expected %d", size, expected)
	}
}

func TestNodeBodySize(t *testing.T) {
	size := nodeBodySize()
	var expected uint32 = 4075
	if size != expected {
		t.Fatalf("Wrong node body size: %d, expected %d", size, expected)
	}
}

func TestTreeHeaderSerialization(t *testing.T) {
	nh := initNodeHeader()

	bytes := nh.serialize()
	nh.deserialize(bytes)

	if nh.Typ != TypeLeaf ||
		nh.Parent != 1 ||
		nh.Next != 3 ||
		nh.CellSize != 542 ||
		nh.Page != 1 ||
		nh.Height != 1 ||
		nh.NumCell != 4 {
		t.Fatal("Failed to serialize node header correctly")
	}
}
