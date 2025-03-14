package btree

import (
	"encoding/hex"
	"fmt"
	"os"
	"testing"

	"github.com/tomial/go-db/internal/constants"
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
	page := makeNodePage(constants.MagicNumberTree)
	err := bt.deserialize(page)
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

func TestInsertIntoEmptyTree(t *testing.T) {
	err := os.Remove("./my.db")
	if err != nil {
		t.Error(err)
	}
	bt := NewBtree()
	defer bt.pager.File.Close()
	buf := make([]byte, 520)
	copy(buf, "Hello World Insert")
	bt.Insert(1, buf)

	bt2 := NewBtree()
	bt2.deserialize(bt.pager.ReadPage(0))
	file, err := os.OpenFile(constants.DbFileName, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		t.Fatal(err)
	}
	fstat, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	expectedFileSize := 2 * int64(constants.PageSize)
	if fstat.Size() != expectedFileSize {
		t.Fatalf("BTree: Failed to create root node and save it to file correctly -- found size %d, expected %d", fstat.Size(), expectedFileSize)
	}

	if bt.First != bt2.First || bt.NumNode != bt2.NumNode || bt.Root != bt2.Root {
		t.Fatalf("BTree: Failed to deserialize tree metadata")
	}

	ln := initEmptyLeafNode()
	ln.deserialize(bt.pager.ReadPage(1))
	if ln.Cells[0].key != 1 || string(ln.Cells[0].data[:18]) != "Hello World Insert" {
		t.Fatalf("BTree: Failed to insert data")
	}
}

func TestInsertAndSplit(t *testing.T) {
	os.Remove("my.db")
	bt := NewBtree()
	buf := make([]byte, 520)
	for i := 1; i <= 17; i++ {
		str := fmt.Sprintf("Hello World Insert %d", i)
		copy(buf, str)
		bt.Insert(uint32(i), buf)
		bt.reload()
	}

	buf = make([]byte, 520)
	copy(buf, "Insert duplicate key 12")
	bt.Insert(12, buf)
	bt.reload()

	if bt.NumNode != 8 || bt.Root != 8 {
		t.Errorf("Failed to insert and split correctly, found num node %d, expected %d; found root %d, expected %d", bt.NumNode, 8, bt.Root, 8)
	}
}
