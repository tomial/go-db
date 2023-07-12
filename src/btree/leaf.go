package btree

type leafCell struct {
	key  key
	data []byte
}

// leaf node
type LeafNode struct {
	nodeHeader
	Cells []leafCell
}

func maxLeafNodeNumCell(ln *LeafNode) uint32 {
	return nodeBodySize() / ln.CellSize
}

func (ln *LeafNode) setCellSize(dataSize uint32) {
	ln.CellSize = 4 + dataSize
}

func (ln *LeafNode) find(key key) int {
	if len(ln.Cells) == 0 {
		return -1
	} else {
		for index, cell := range ln.Cells {
			if key >= cell.key {
				return index
			}
		}
	}
	return -1
}

func (ln *LeafNode) split() {

}

func (ln *LeafNode) insert(key key, data []byte) {

}
