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
	Header *nodeHeader
	Cells  []*internalCell
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
	return 2
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

func (in *InternalNode) serializeCells() ([]byte, error) {
	cells := in.Cells
	if len(cells) == 0 {
		return nil, errors.New("serializing internal node: empty internal node")
	}
	cellsBytes := make([]byte, maxInternalNodeNumCell()*internalNodeCellSize())
	var pos uint32 = 0

	for _, cell := range cells {
		binary.LittleEndian.PutUint32(cellsBytes[pos:], uint32(cell.key))
		pos = util.AdvanceCursor(pos, 4)
		binary.LittleEndian.PutUint32(cellsBytes[pos:], uint32(cell.left))
		pos = util.AdvanceCursor(pos, 4)
		binary.LittleEndian.PutUint32(cellsBytes[pos:], uint32(cell.right))
		pos = util.AdvanceCursor(pos, 4)
	}

	return cellsBytes, nil
}

func (in *InternalNode) deserializeCells(bytes []byte) error {
	size := len(bytes)
	if uint32(size) != uint32(in.Header.NumCell)*internalNodeCellSize() {
		return errors.New("deserializing internal node cells: invalid size of cells bytes")
	}

	in.Cells = make([]*internalCell, in.Header.NumCell)

	var pos uint32 = 0
	for i := 0; i < int(in.Header.NumCell); i++ {
		key := key(binary.LittleEndian.Uint32(bytes[pos : pos+4]))
		pos = util.AdvanceCursor(pos, 4)
		left := PageNum(binary.LittleEndian.Uint32(bytes[pos : pos+4]))
		pos = util.AdvanceCursor(pos, 4)
		right := PageNum(binary.LittleEndian.Uint32(bytes[pos : pos+4]))
		pos = util.AdvanceCursor(pos, 4)
		in.Cells[i] = &internalCell{
			key:   key,
			left:  left,
			right: right,
		}
	}

	return nil
}
