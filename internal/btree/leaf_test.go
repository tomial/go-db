package btree

import (
	"encoding/binary"
	"encoding/hex"
	"testing"

	"github.com/tomial/go-db/internal/constants"
	"github.com/tomial/go-db/internal/util"
)

func TestLeafNodeCellSize(t *testing.T) {
	ln := initLeafNode()
	ln.SetCellSize(520)
	size := ln.Header.CellSize
	expected := 524 // key + table row size
	if size != uint32(expected) {
		t.Fatalf("Wrong leaf node cell size: %d, expected %d", size, expected)
	}
}

func TestMaxLeafNodeNumCell(t *testing.T) {
	lf := initLeafNode()
	lf.SetCellSize(520)
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

func initLeafNode() *LeafNode {
	ln := &LeafNode{Header: initNodeHeader()}
	ln.SetCellSize(520)
	testBytes := make([]byte, 520)
	testBytes[0] = 0xAB
	testBytes[1] = 0xCD
	ln.Header.NumCell = 3
	ln.Cells = []*leafCell{
		{
			key:  1,
			data: testBytes,
		},
		{
			key:  2,
			data: testBytes,
		},
		{
			key:  3,
			data: testBytes,
		},
	}
	return ln
}

func TestSerializeLeafCells(t *testing.T) {
	ln := initLeafNode()
	bytes, err := ln.serializeCells()
	if err != nil {
		t.Fatal(err)
	}
	if len(bytes) != int(ln.Header.CellSize)*3 {
		t.Fatalf("Serialize leaf cells: invalid cell size -- %d, expected %d\n", len(bytes), ln.Header.CellSize*3)
	}

	pos := 0
	keyBytes := bytes[pos : pos+constants.BTreeKeySize]
	key := binary.LittleEndian.Uint32(keyBytes)
	pos = util.AdvanceCursor(pos, constants.BTreeKeySize)
	if key != 1 || bytes[pos] != 0xAB || bytes[pos+1] != 0xCD {
		t.Fatalf("Serialize leaf cells: invalid cell bytes, found %v %v, expected %v %v", bytes[pos], bytes[pos+1], 0xAB, 0xCD)
	}

	pos = int(ln.Header.CellSize)
	keyBytes = bytes[pos : pos+constants.BTreeKeySize]
	key = binary.LittleEndian.Uint32(keyBytes)
	pos = util.AdvanceCursor(pos, constants.BTreeKeySize)
	if key != 2 || bytes[pos] != 0xAB || bytes[pos+1] != 0xCD {
		t.Fatalf("Serialize leaf cells: invalid cell bytes, found %v %v, expected %v %v", bytes[pos], bytes[pos+1], 0xAB, 0xCD)
	}
}

func TestLeafNodeSerialization(t *testing.T) {
	ln := initLeafNode()
	bin := ln.serialize()
	ln1 := &LeafNode{
		Header: &nodeHeader{},
	}
	err := ln1.deserialize(bin)
	if err != nil {
		t.Error(err)
	}

	if ln.Header.Typ != ln1.Header.Typ ||
		ln.Header.Height != ln1.Header.Height ||
		ln.Header.Next != ln1.Header.Next ||
		ln.Header.NumCell != ln1.Header.NumCell ||
		ln.Header.Parent != ln1.Header.Parent ||
		ln.Header.CellSize != ln1.Header.CellSize ||
		ln.Header.Page != ln1.Header.Page ||
		ln.Cells[0].key != ln1.Cells[0].key ||
		ln.Cells[1].key != ln1.Cells[1].key ||
		ln.Cells[2].key != ln1.Cells[2].key ||
		ln.Cells[0].data[0] != ln1.Cells[0].data[0] ||
		ln.Cells[0].data[1] != ln1.Cells[0].data[1] {
		t.Fatalf("Testing leaf node serialization: cannot serialize and deserialize leaf node correctly")
	}
}
