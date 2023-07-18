package btree

import (
	"db/src/constants"
	"db/src/util"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"reflect"
)

type internalCell struct {
	key   key
	left  PageNum
	right PageNum
}

// internal node
type InternalNode struct {
	btree  *BTree
	Header *nodeHeader
	Cells  []*internalCell
}

func initEmptyInternalNode() *InternalNode {
	in := &InternalNode{Header: initHeader(TypeInternal)}
	in.Header.CellSize = internalNodeCellSize()
	return in
}

// The cell size of internal node is fixed
func internalNodeCellSize() uint32 {
	ic := internalCell{}
	val := reflect.ValueOf(ic)
	num := val.NumField()

	var total uint32 = 0
	for i := 0; i < num; i++ {
		total += uint32(val.Field(i).Type().Size())
	}

	return total
}

func maxInternalNodeNumCell() uint32 {
	// returns 340, that's 680 children for an internal, too many for testing
	// return nodeBodySize() / internalNodeCellSize()
	// limit the children size, 4 children per node
	return 3
}

func (in *InternalNode) find(key key) (found bool, data []byte) {
	for index, cell := range in.Cells {
		// The last cell
		if index == len(in.Cells)-1 && key >= cell.key {
			return in.btree.readNode(cell.right).find(key)
		}

		if cell.key >= key {
			return in.btree.readNode(cell.left).find(key)
		}
	}
	return false, nil
}

func (in *InternalNode) serialize() []byte {
	page := makeNodePage(constants.MagicNumberInternal)

	pos := constants.MagicNumberSize
	headerBytes := in.Header.serialize()
	copy(page[pos:pos+nodeHeaderSize()], headerBytes)
	pos = util.AdvanceCursor(pos, nodeHeaderSize())

	cellsBytes, err := in.serializeCells()
	copy(page[pos:pos+nodeBodySize()], cellsBytes)
	if err != nil {
		log.Fatalln(err)
	}

	return page
}

func (in *InternalNode) deserialize(bytes []byte) error {
	if len(bytes) != int(constants.PageSize) {
		return fmt.Errorf("deserializing internal node: invalid bytes size -- %d, expected %d", len(bytes), constants.PageSize)
	}

	magicNumber := hex.EncodeToString(bytes[:constants.MagicNumberSize])
	if magicNumber != constants.MagicNumberInternal {
		return fmt.Errorf("deserializing internal node: invalid magic number for internal node -- %s, expected %s", magicNumber, constants.MagicNumberInternal)
	}

	pos := constants.MagicNumberSize
	err := in.Header.deserialize(bytes[pos : pos+nodeHeaderSize()])
	if err != nil {
		return err
	}
	pos = util.AdvanceCursor(pos, nodeHeaderSize())
	err = in.deserializeCells(bytes[pos : pos+uint32(in.Header.NumCell)*internalNodeCellSize()])
	if err != nil {
		return err
	}

	return nil
}

func (ic *internalCell) serialize() ([]byte, error) {
	if ic == nil {
		return nil, errors.New("internal node serialization: the cell pointer is nil")
	}

	pos := 0
	buf := make([]byte, internalNodeCellSize())
	binary.LittleEndian.PutUint32(buf[pos:], uint32(ic.key))
	pos = util.AdvanceCursor(pos, 4)
	binary.LittleEndian.PutUint32(buf[pos:], uint32(ic.left))
	pos = util.AdvanceCursor(pos, 4)
	binary.LittleEndian.PutUint32(buf[pos:], uint32(ic.right))

	return buf, nil
}

func (in *InternalNode) serializeCells() ([]byte, error) {
	cells := in.Cells
	if len(cells) == 0 {
		return nil, errors.New("serializing internal node: empty internal node")
	}
	cellsBytes := make([]byte, maxInternalNodeNumCell()*internalNodeCellSize())
	var pos uint32 = 0

	for _, cell := range cells {
		if cell != nil {
			bytes, err := cell.serialize()
			if err != nil {
				return nil, err
			}
			copy(cellsBytes[pos:], bytes)
			pos = util.AdvanceCursor(pos, internalNodeCellSize())
		}
	}

	return cellsBytes, nil
}

func (ic *internalCell) deserialize(bytes []byte) error {
	if len(bytes) != int(internalNodeCellSize()) {
		return fmt.Errorf("deserializing internal cell: invalid byte size %d, expected %d", len(bytes), internalNodeCellSize())
	}

	pos := 0
	ic.key = key(binary.LittleEndian.Uint32(bytes[pos : pos+4]))
	pos = util.AdvanceCursor(pos, 4)
	ic.left = PageNum(binary.LittleEndian.Uint32(bytes[pos : pos+4]))
	pos = util.AdvanceCursor(pos, 4)
	ic.right = PageNum(binary.LittleEndian.Uint32(bytes[pos : pos+4]))

	return nil
}

func (in *InternalNode) deserializeCells(bytes []byte) error {
	size := len(bytes)
	if uint32(size) != uint32(in.Header.NumCell)*internalNodeCellSize() {
		return errors.New("deserializing internal node cells: invalid size of cells bytes")
	}

	// internal node need to insert before split, add one more slot here
	in.Cells = make([]*internalCell, maxInternalNodeNumCell()+1)

	var pos uint32 = 0
	for i := 0; i < int(in.Header.NumCell); i++ {
		in.Cells[i] = &internalCell{}
		err := in.Cells[i].deserialize(bytes[pos : pos+internalNodeCellSize()])
		pos = util.AdvanceCursor(pos, internalNodeCellSize())
		if err != nil {
			return err
		}
	}

	return nil
}

func (in *InternalNode) syncNeighborPointer(index int) {
	// Sync if there's a right cell
	if uint8(index) < in.Header.NumCell-1 {
		in.Cells[index+1].left = in.Cells[index].right
	}
	// Sync the left cell
	if index > 0 {
		in.Cells[index-1].right = in.Cells[index].left
	}
}

func (in *InternalNode) searchLeaf(key key) *LeafNode {
	for index, cell := range in.Cells {
		// The last cell
		if uint8(index) == in.Header.NumCell-1 && key > cell.key {
			if in.btree == nil {
				in.btree = NewBtree()
			}
			return in.btree.readNode(cell.right).searchLeaf(key)
		}

		if cell.key >= key {
			if in.btree == nil {
				in.btree = NewBtree()
			}
			return in.btree.readNode(cell.left).searchLeaf(key)
		}
	}
	return nil
}

func (in *InternalNode) split() *InternalNode {
	if in.btree == nil {
		in.btree = NewBtree()
	}

	right := initEmptyInternalNode()
	right.Header.CellSize = in.Header.CellSize
	right.Header.Page = PageNum(in.btree.NumNode + 1)
	right.Header.Parent = in.Header.Parent
	in.btree.NumNode++
	in.Header.Next = right.Header.Page
	right.Header.Typ = TypeInternal
	right.Cells = make([]*internalCell, maxInternalNodeNumCell()+1)

	middle := in.Header.NumCell / 2
	bubbleKey := in.Cells[middle].key

	// the middle cell goes to parent node
	copy(right.Cells, in.Cells[middle+1:])
	right.Header.NumCell = in.Header.NumCell - middle - 1 // minus nodes after middle and the one bubbled into parent
	for index := range in.Cells[middle:] {
		in.Cells[index] = nil
		in.Header.NumCell--
	}

	if in.Header.Parent != 0 {
		parentNode := in.btree.readNode(in.Header.Parent)
		bubbleCell := &internalCell{
			key:   bubbleKey,
			left:  in.Header.Page,
			right: right.Header.Page,
		}
		bubbleBytes, err := bubbleCell.serialize()
		if err != nil {
			log.Fatal(err)
		}
		parentNode.saveCell(bubbleKey, bubbleBytes)
	} else {
		// the bubble key goes into new root node
		if in.Header.Typ == TypeRoot {
			in.Header.Typ = TypeInternal
		}
		newRoot := initEmptyInternalNode()
		newRoot.btree = in.btree

		newRoot.Header.Typ = TypeRoot
		newRoot.Header.CellSize = internalNodeCellSize()
		newRoot.Header.Page = PageNum(in.btree.NumNode) + 1
		in.btree.Root = newRoot.Header.Page
		in.btree.NumNode++
		newRoot.Header.Parent = 0
		in.Header.Parent = newRoot.Header.Page
		right.Header.Parent = newRoot.Header.Page
		right.Header.Parent = newRoot.Header.Page

		newRoot.Cells = make([]*internalCell, maxInternalNodeNumCell()+1)
		newRoot.Cells[0] = &internalCell{
			key:   bubbleKey,
			left:  in.Header.Page,
			right: right.Header.Page,
		}
		newRoot.Header.NumCell++
		newRoot.btree.save()
		newRoot.save()
	}

	return right
}

func (in *InternalNode) saveCell(key key, data []byte) {
	ic := &internalCell{}
	err := ic.deserialize(data)
	if err != nil {
		log.Fatal(err)
	}

	pos := 0
	for index, cell := range in.Cells {
		if cell == nil || in.Cells[index].key > key {
			pos = index
			break
		}
	}

	if in.Cells[pos] != nil {
		copy(in.Cells[pos+1:], in.Cells[pos:])
	}

	in.Cells[pos] = ic
	in.Header.NumCell++
	in.syncNeighborPointer(pos)

	// add cell to internal node before split
	if in.Header.NumCell == uint8(maxInternalNodeNumCell()+1) {
		right := in.split()
		right.save()
	}
	in.save()

}

func (in *InternalNode) save() error {
	if in.btree == nil {
		in.btree = NewBtree()
	}

	if in.Header.Page == 0 {
		return fmt.Errorf("saving internal node: invalid node page: %d", in.Header.Page)
	}

	in.btree.pager.WritePage(uint32(in.Header.Page), in.serialize())

	return nil
}
