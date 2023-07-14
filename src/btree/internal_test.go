package btree

import (
	"db/src/constants"
	"encoding/hex"
	"testing"
)

func initInternalNode() *InternalNode {
	return &InternalNode{Header: initNodeHeader()}
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
	expected := 2
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
