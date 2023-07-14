package btree

import (
	"db/src/constants"
	"encoding/hex"
	"testing"
)

func TestBTreeStructSize(t *testing.T) {
	bt := &BTree{}
	size := bt.structSize()
	var expected uint = 12
	if size != expected {
		t.Errorf("Wrong btree struct size %d, expected %d\n", size, expected)
	}
}

func TestBTreeSerialization(t *testing.T) {
	bt := &BTree{Root: 123, First: 321, NumNode: 111}
	bin := bt.serialize()
	// reset values
	bt.Root = 0
	bt.First = 0
	bt.NumNode = 0
	err := bt.deserialize(bin)
	if err != nil {
		t.Error(err.Error())
	}
	var expectedRoot PageNum = 123
	var expectedFirst PageNum = 321
	var expectedNumNode uint32 = 111
	if bt.Root != expectedRoot || bt.First != expectedFirst || bt.NumNode != expectedNumNode {
		t.Errorf("Serialize btree: Wrong root %d and first %d numNode %d, expected %d and %d and %d\n", bt.Root, bt.First, bt.NumNode, expectedRoot, expectedFirst, expectedNumNode)
	}
}

func TestBTreeDeserializationError(t *testing.T) {
	bt := &BTree{}
	bin := make([]byte, 12) // random binary
	err := bt.deserialize(bin)
	if err != nil {
		t.Error("Deserialize btree: failed to capture error")
	}
}

func TestMakeTreeNodeEmptyPage(t *testing.T) {
	buf := makeNodePage(constants.MagicNumberTree)
	magicNumberStr := hex.EncodeToString(buf[:constants.MagicNumberSize])
	if magicNumberStr != constants.MagicNumberTree || magicNumberStr == constants.MagicNumberLeaf {
		t.Fatalf("Failed to make leaf page: %s, expected %s\n", magicNumberStr, constants.MagicNumberTree)
	}
}
