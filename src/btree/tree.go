package btree

import (
	"db/src/constants"
	"db/src/pager"
	"db/src/util"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"reflect"
)

type key uint32

// How to build a btree:
// New file:
// Create a tree struct and a root node, save it to file

// Existing file:
// Read tree struct from file
// +------+------+------+------+------+------------+ -> db file
// | 4KB  | 4KB  | 4KB  | 4KB  | 4KB  |            |
// |      |      |      |      |      |            |
// | Tree | Node | Node | Node | Node |            |
// +------+------+------+------+------+------------+

type BTree struct {
	Root    PageNum // Root node's page num
	First   PageNum // Leftmost leaf node, for iteration
	NumNode uint32
	pager   *pager.Pager
}

func NewBtree() *BTree {
	file, err := os.OpenFile(constants.DbFileName, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		log.Fatalf("BTree: failed to open database file %s -- %s", constants.DbFileName, err)
	}
	fstat, err := file.Stat()
	if err != nil {
		log.Fatalf(("New btree: failed to get db file stat -- %s\n"), err.Error())
	}
	file.Seek(0, io.SeekStart)
	bt := &BTree{Root: 0, First: 0, NumNode: 0, pager: pager.Init(file)} // No root and first node
	if fstat.Size() == 0 {                                               // New file
		bin := bt.serialize()
		_, err := file.Write(bin)
		if err != nil {
			log.Fatalf("Creating btree: failed to write btree binary to db file -- %s", err.Error())
		}
	} else { // Existing file
		bt.loadTree()
	}
	return bt
}

func (bt *BTree) structSize() uint {
	val := reflect.ValueOf(bt)
	elem := val.Elem()
	num := elem.NumField()
	var total uint = 0
	for i := 0; i < num; i++ {
		field := elem.Field(i)
		if field.Type().Kind() == reflect.Uint32 {
			total += uint(field.Type().Size())
		}
	}
	return total
}

func (bt *BTree) serialize() []byte {
	buf := make([]byte, bt.structSize())
	val := reflect.ValueOf(bt)
	elem := val.Elem()
	num := elem.NumField()
	pos := 0
	for i := 0; i < num; i++ {
		field := elem.Field(i)
		// don't serialize pointers
		if field.Type().Kind() == reflect.Uint32 {
			binary.LittleEndian.PutUint32(buf[pos:], uint32(elem.Field(i).Uint()))
			pos = util.AdvanceCursor(pos, 4)
		}
	}
	treePage := makeNodePage(constants.MagicNumberTree)
	copy(treePage[constants.MagicNumberSize:], buf)
	return treePage
}

func (bt *BTree) deserialize(page []byte) error {
	pageSize := len(page)
	if pageSize != int(constants.PageSize) {
		return fmt.Errorf("deserializing btree: wrong btree page size %d, expected %d", pageSize, constants.PageSize)
	} else {
		data := page[constants.MagicNumberSize:]
		bt.Root = PageNum(binary.LittleEndian.Uint32(data[:4]))
		bt.First = PageNum(binary.LittleEndian.Uint32(data[4:8]))
		bt.NumNode = binary.LittleEndian.Uint32(data[8:])
		return nil
	}
}

func (bt *BTree) Insert(key key, data []byte) {
	// Empty Tree
	// Create a root node to page 1 and insert
	if bt.Root == 0 {
		root := createRootNode(data)
		root.saveCell(key, data)
		bt.Root = 1
		bt.NumNode += 1
		bt.First = 1
		bt.save()
	} else {
		// find the leaf node and insert it
		ln := bt.searchLeaf(key)
		if ln == nil {
			log.Fatalln("BTree insert: failed to find leaf node to insert")
		}
		// If the node split, the original page would be changed
		ln.saveCell(key, data)
	}
}

func (bt *BTree) searchLeaf(key key) *LeafNode {
	node := bt.readNode(bt.Root)
	return node.searchLeaf(key)
}

func createRootNode(data []byte) *LeafNode {
	ln := initEmptyRootNode()
	ln.SetCellSize(uint32(len(data)))
	ln.Cells = make([]*leafCell, ln.maxLeafNodeNumCell())
	ln.Header.Page = 1
	return ln
}

func (bt *BTree) Search(key key) (found bool, data []byte) {
	if bt.NumNode == 0 {
		return false, nil
	}
	page := bt.pager.ReadPage(uint32(bt.Root))
	switch nodeType(page) {
	case TypeLeaf:
		{
			ln := initEmptyLeafNode()
			ln.deserialize(page)
			return ln.find(key)
		}
	case TypeInternal:
		{
			in := initEmptyInternalNode()
			in.deserialize(page)
			return in.find(key)
		}
	}
	return false, nil
}

func (bt *BTree) Delete(key key) {}

func FullScan() {}

func nodeType(page []byte) NodeType {
	typ := hex.EncodeToString(page[:constants.MagicNumberSize])
	switch typ {
	case constants.MagicNumberLeaf:
		{
			return TypeLeaf
		}
	case constants.MagicNumberInternal:
		{
			return TypeInternal
		}
	default:
		{
			return TypeInvalid
		}
	}
}

func (bt *BTree) readNode(page PageNum) node {
	bytes := bt.pager.ReadPage(uint32(page))
	switch nodeType(bytes) {
	case TypeLeaf:
		{
			ln := initEmptyLeafNode()
			err := ln.deserialize(bytes)
			if err != nil {
				log.Fatal(err)
			}
			return ln
		}
	case TypeInternal:
		{
			in := initEmptyInternalNode()
			err := in.deserialize(bytes)
			if err != nil {
				log.Fatal(err)
			}
			return in
		}
	default:
		{
			return nil
		}
	}
}

// save tree metadata
func (bt *BTree) save() {
	bytes := bt.serialize()
	bt.pager.WritePage(0, bytes)
}

func (bt *BTree) loadTree() {
	bin := make([]byte, constants.PageSize)
	_, err := bt.pager.File.Read(bin)
	if err != nil {
		log.Fatalf("Creating btree: failed to read from db file -- %s", err.Error())
	}
	err = bt.deserialize(bin)
	if err != nil {
		log.Fatal(err)
	}
}

func (bt *BTree) reload() {
	if bt.pager.File == nil {
		file, err := os.OpenFile(constants.DbFileName, os.O_RDWR|os.O_CREATE, 0755)
		if err != nil {
			log.Fatalf("reloading btree: failed to open database file %s -- %s", constants.DbFileName, err)
		}
		bt.pager.File = file
	}
	bytes := bt.pager.ReadPage(0)
	bt.deserialize(bytes)
}

// TODO Draw the whole tree
func Visualize() {}
