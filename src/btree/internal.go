package btree

import "reflect"

type internalCell struct {
	key   key
	left  PageNum
	right PageNum
}

// internal node
type InternalNode struct {
	nodeHeader
	children []internalCell
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
