package btree

import (
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
	if bt.Root != expectedRoot && bt.First != expectedFirst && bt.NumNode != expectedNumNode {
		t.Errorf("Serialize btree: Wrong root %d and first %d, expected %d and %d\n", bt.Root, bt.First, expectedRoot, expectedFirst)
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