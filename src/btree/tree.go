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

func NewBtree(file *os.File) *BTree {
	fstat, err := file.Stat()
	if err != nil {
		log.Fatalf(("New btree: failed to get db file stat -- %s\n"), err.Error())
	}
	file.Seek(0, io.SeekStart)
	bt := &BTree{Root: 0, First: 0, NumNode: 0, pager: pager.Init()} // No root and first node
	if fstat.Size() == 0 {                                           // New file
		bin := bt.serialize()
		_, err := file.Write(bin)
		if err != nil {
			log.Fatalf("Creating btree: failed to write btree binary to db file -- %s", err.Error())
		}
	} else { // Existing file
		bin := make([]byte, bt.structSize())
		_, err := file.Read(bin)
		if err != nil {
			log.Fatalf("Creating btree: failed to read from db file -- %s", err.Error())
		}
		bt.deserialize(bin)
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
	return buf
}

func (bt *BTree) deserialize(bin []byte) error {
	binSize := len(bin)
	if binSize != int(bt.structSize()) {
		return fmt.Errorf("deserializing btree: wrong btree size %d, expected %d", binSize, bt.structSize())
	} else {
		bt.Root = PageNum(binary.LittleEndian.Uint32(bin[:4]))
		bt.First = PageNum(binary.LittleEndian.Uint32(bin[4:8]))
		bt.NumNode = binary.LittleEndian.Uint32(bin[8:])
		return nil
	}
}

// TODO Draw the whole tree
func Visualize() {}

// TODO Search pos
func Insert(key key, data []byte, typ NodeType) {

}

func (bt *BTree) Search(key key) (found bool, data []byte) {
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
	switch hex.EncodeToString(page[:constants.MagicNumberSize]) {
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
			ln.deserialize(bytes)
			return ln
		}
	case TypeInternal:
		{
			in := initEmptyInternalNode()
			in.deserialize(bytes)
			return in
		}
	default:
		{
			return nil
		}
	}
}
